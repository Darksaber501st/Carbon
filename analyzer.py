"""
Analyzer for MODIS satellite data using parallel processing.
Processes downloaded HDF files and calculates vegetation indices.
"""

import os
import json
import datetime, time
import numpy as np
import h5py
import rasterio
from rasterio.mask import mask
import mysql.connector
from mysql.connector import Error
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple, Union
from shapely.geometry import Polygon, Point
import tempfile
import shutil

# Configure logging
logging.basicConfig(
    filename=f'/var/log/carbon/satellite_analyzer_{datetime.datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SatelliteAnalyzer:
    def __init__(self, config_path: str):
        """Initialize the analyzer with configuration settings."""
        self.config = self._load_config(config_path)
        self.db_connection = self._init_db_connection()
        self.debug = self.config.get('debug', False)
        if self.debug:
            logger.info("Debug mode activated")
            logger.setLevel(logging.DEBUG)
        
        # Create processed directory if it doesn't exist
        os.makedirs(self.config['processed_dir'], exist_ok=True)
        
        # Create temporary processing directory
        self.temp_dir = tempfile.mkdtemp()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        with open(config_path) as f:
            return json.load(f)

    def _init_db_connection(self) -> mysql.connector.connection.MySQLConnection:
        """Initialize database connection."""
        try:
            return mysql.connector.connect(
                host=self.config['db']['host'],
                user=self.config['db']['user'],
                password=self.config['db']['password'],
                database=self.config['db']['database']
            )
        except Error as e:
            logger.error(f"Error connecting to database: {e}", exc_info=True)
            raise

    def get_downloaded_jobs(self) -> List[Dict]:
        """Fetch jobs that have completed downloads and need analysis."""
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM analysis_jobs 
            WHERE download_status = 'downloaded' 
            AND (analysis_status IS NULL OR analysis_status = 'pending')
            ORDER BY created_at
        """)
        results = cursor.fetchall()
        logger.debug(f"Found {len(results)} downloaded jobs")
        self.db_connection.commit()
        return results

    def process_tiff_file(self,args: Tuple) -> Dict:
        """
        Process a single GeoTIFF file. This function runs in a separate process.
        Returns NDVI statistics for the given geometry.
        
        Args:
            args: Tuple containing (file_path, geometry_dict, temp_dir)
        Returns:
            Dictionary containing date and NDVI statistics
        """
        file_path, geometry_dict, temp_dir = args
        
        try:
            with rasterio.open(file_path) as src:
                # Create geometry mask
                if geometry_dict['geometry']['type'] == 'Point':
                    point = Point(geometry_dict['geometry']['coordinates'])
                    buffer_distance = 0.008333  # ~0.5 miles in degrees
                    geometry_mask = point.buffer(buffer_distance)
                    
                elif geometry_dict['geometry']['type'] == 'Polygon':
                    geometry_mask = Polygon(geometry_dict['geometry']['coordinates'][0])
                else:
                    raise ValueError(f"Invalid geometry type: '{geometry_dict['geometry']['type']}', valid types are 'Point' and 'Polygon'")
                
                # Mask the data to the geometry
                masked_data, masked_transform = mask(src, [geometry_mask], crop=True)
                
                # Calculate statistics for the masked region
                valid_data = masked_data[0][masked_data[0] != src.nodata]
                
                if len(valid_data) > 0:
                    # MODIS NDVI scale factor is 0.0001
                    stats = {
                        'date': self._extract_date_from_filename(file_path),
                        'mean_ndvi': float(np.mean(valid_data) * 0.0001),
                        'min_ndvi': float(np.min(valid_data) * 0.0001),
                        'max_ndvi': float(np.max(valid_data) * 0.0001),
                        'std_ndvi': float(np.std(valid_data) * 0.0001)
                    }
                    return stats
                
                return None
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
            return None

    def _extract_date_from_filename(self,filepath: str) -> datetime.date:
        """
        Extract date from AppEEARS MODIS filename.
        Example: 'aef3634b-2141-47d3-b746-7a8c639b2a53_MOD13Q1.061__250m_16_days_NDVI_doy2024033_aid0001.tif'
        
        Args:
            filepath: Path to the file
        Returns:
            datetime.date object for the file's date
        """
        filename = os.path.basename(filepath)
        # Find the part that starts with 'doy' and extract the year and day of year
        doy_part = [part for part in filename.split('_') if part.startswith('doy')][0]
        year = int(doy_part[3:7])
        day_of_year = int(doy_part[7:10])
        
        return datetime.datetime(year, 1, 1) + datetime.timedelta(days=day_of_year - 1)

    def update_job_status(self, job_id: int, level: str, status: str, message: str = None) -> None:
        """Update job status in database."""
        cursor = self.db_connection.cursor()
        if level == 'main':
            if message:
                logger.warning("Message included in 'main' status update. Ignoring...")
            cursor.execute(
                """
                UPDATE analysis_jobs 
                SET status = %s
                WHERE id = %s
                """,
                (status, job_id)
            )
            self.db_connection.commit()
        elif level == 'analysis':
            cursor.execute(
                """
                UPDATE analysis_jobs 
                SET analysis_status = %s, analysis_message = %s
                WHERE id = %s
                """,
                (status, message, job_id)
            )
            self.db_connection.commit()
        else:
            raise ValueError(f"Invalid status level: {level}")

    def process_job(self, job: Dict) -> None:
        """Process a single job's analysis."""
        try:
            logger.info(f"Analyzing job {job['id']}")
            self.update_job_status(job_id=job['id'], level="analysis",status='analyzing')
            
            # Get list of downloaded files
            job_dir = os.path.join(self.config['download_dir'], f"job_{job['id']}")
            if not os.path.exists(job_dir):
                raise FileNotFoundError(f"Job directory not found: {job_dir}")
            
            #hdf_files = [
            #    os.path.join(job_dir, f) for f in os.listdir(job_dir)
            #    if f.endswith('.hdf')
            #]
            tif_files = [
                os.path.join(job_dir, f) for f in os.listdir(job_dir)
                if f.endswith('.tif')
            ] 
            
            if not tif_files:
                raise ValueError("No TIF files found for processing")
            
            # Prepare parallel processing tasks
            geometry = json.loads(job['geometry'])
            process_args = [
                (file_path, geometry, self.temp_dir)
                for file_path in tif_files
            ]
            
            # Process files in parallel
            results = []
            max_workers = self.config.get('max_concurrent_processes', os.cpu_count())
            
            logger.debug(f"Parallel processing {len(tif_files)} files...")
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(self.process_tiff_file, args): args[0]
                    for args in process_args
                }
                
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing {file_path}: {str(e)}")
            
            if not results:
                raise ValueError("No valid results obtained from processing")
            
            logger.debug(f"Finished processing {len(results)} files...")
            # Calculate vegetation changes
            changes = self._calculate_vegetation_changes(results)
            
            # Save results
            self._save_results(job['id'], changes)
            
            # Update status to completed
            self.update_job_status(job_id=job['id'], level="analysis",status='completed',message=f"Successfully analyzed {len(results)} files")
            logger.info(f"Finished analyzing job {job['id']}")
        except Exception as e:
            logger.error(f"Error analyzing job {job['id']}: {str(e)}", exc_info=True)
            self.update_job_status(job_id=job['id'], level="analysis",status='failed',message=str(e))

    def _calculate_vegetation_changes(
        self,
        results: List[Dict[str, Union[datetime.date, float]]]
    ) -> Dict:
        """Calculate vegetation change metrics from NDVI time series."""
        logger.debug("Calculating vegetation changes...")
        if not results:
            return {
                'overall_change': 0,
                'trend': 0,
                'timeseries': []
            }
        
        # Sort by date
        results.sort(key=lambda x: x['date'])
        
        # Calculate overall change
        overall_change = results[-1]['mean_ndvi'] - results[0]['mean_ndvi']
        
        # Calculate trend using linear regression
        dates = np.array([(r['date'] - results[0]['date']).days for r in results])
        ndvi_values = np.array([r['mean_ndvi'] for r in results])
        
        if len(dates) > 1:
            trend = np.polyfit(dates, ndvi_values, 1)[0] * 365  # Convert to per-year
        else:
            trend = 0
        
        # Calculate additional statistics
        max_ndvi = max(r['max_ndvi'] for r in results)
        min_ndvi = min(r['min_ndvi'] for r in results)
        mean_std = np.mean([r['std_ndvi'] for r in results])
        
        return {
            'overall_change': float(overall_change),
            'trend': float(trend),
            'max_ndvi': float(max_ndvi),
            'min_ndvi': float(min_ndvi),
            'mean_std': float(mean_std),
            'timeseries': [
                {
                    'date': r['date'].isoformat(),
                    'mean_ndvi': r['mean_ndvi'],
                    'min_ndvi': r['min_ndvi'],
                    'max_ndvi': r['max_ndvi'],
                    'std_ndvi': r['std_ndvi']
                }
                for r in results
            ]
        }

    def _save_results_OLD(self, job_id: int, results: Dict) -> None:
        """Save analysis results to database."""
        logger.debug(f"Saving results for job {job_id}")
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO job_results 
                (job_id, overall_change, trend, max_ndvi, min_ndvi, 
                 mean_std, timeseries_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                results['overall_change'],
                results['trend'],
                results['max_ndvi'],
                results['min_ndvi'],
                results['mean_std'],
                json.dumps(results['timeseries'])
            )
        )
        self.db_connection.commit()

    def _save_results(self, job_id: int, results: Dict) -> None:
        """Save analysis results to database with connection retry."""
        logger.debug(f"Saving results for job {job_id}")
        try:
            # Check if connection is alive
            if not self.db_connection.is_connected():
                logger.info("Reconnecting to database...")
                self.db_connection.reconnect(attempts=3, delay=0)

            cursor = self.db_connection.cursor()
            cursor.execute(
                """
                INSERT INTO job_results 
                    (job_id, overall_change, trend, max_ndvi, min_ndvi, 
                    mean_std, timeseries_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job_id,
                    results['overall_change'],
                    results['trend'],
                    results['max_ndvi'],
                    results['min_ndvi'],
                    results['mean_std'],
                    json.dumps(results['timeseries'])
                )
            )
            self.db_connection.commit()
            
        except Error as e:
            logger.error(f"Database error in _save_results: {e}")
            # Try to reconnect and retry once
            try:
                self.db_connection = self._init_db_connection()
                cursor = self.db_connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO job_results 
                        (job_id, overall_change, trend, max_ndvi, min_ndvi, 
                        mean_std, timeseries_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        results['overall_change'],
                        results['trend'],
                        results['max_ndvi'],
                        results['min_ndvi'],
                        results['mean_std'],
                        json.dumps(results['timeseries'])
                    )
                )
                self.db_connection.commit()
            except Error as retry_error:
                logger.error(f"Retry failed in _save_results: {retry_error}", exc_info=True)
                raise

    def run(self):
        """Main processing loop."""
        try:
            while True:
                pending_jobs = self.get_downloaded_jobs()
                if not pending_jobs:
                    logger.info("No pending jobs found, sleeping...")
                    time.sleep(10)
                    continue
                
                for job in pending_jobs:
                    self.process_job(job)
                
        except KeyboardInterrupt:
            logger.info("Shutting down analyzer...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources and close connections."""
        if hasattr(self, 'db_connection') and self.db_connection.is_connected():
            self.db_connection.close()
        
        # Clean up temporary directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

if __name__ == '__main__':
    analyzer = SatelliteAnalyzer('/var/www/carbon/python/config.json')
    time.sleep(10)  # Wait for database to initialize
    analyzer.run()