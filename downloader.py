"""
Download manager for MODIS satellite data using NASA AppEEARS API.
"""

import os
import json
import datetime
import requests
import time
from typing import Dict, List, Tuple
import mysql.connector
from mysql.connector import Error
import logging
from shapely.geometry import Polygon, Point
import geopandas as gpd

# Configure logging
logging.basicConfig(
    filename=f'/var/log/carbon/satellite_downloader_{datetime.datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MODISDownloader:
    def __init__(self, config_path: str):
        """Initialize the downloader with configuration settings."""
        self.config = self._load_config(config_path)
        self.db_connection = self._init_db_connection()
        self.api_url = 'https://appeears.earthdatacloud.nasa.gov/api/'
        self.debug = self.config.get('debug', False)
        if self.debug:
            logger.info("Debug mode activated")
            logger.setLevel(logging.DEBUG)

        
        # Get authentication headers
        #self.headers = self.check_for_active_token()
        #if self.headers is None:
        #    self.headers = self.open_connection()
        self.headers = self.open_connection()
        # Create download directory if it doesn't exist
        os.makedirs(self.config['download_dir'], exist_ok=True)
        
        # Data product definition
        self.data_defs = {
            "MODIS": {
                "productsAndLayers": [
                    {
                        "layer": '_250m_16_days_NDVI', #TODO: make configurable based on web input
                        "product": 'MOD13Q1.061'
                    }
                ]
            }
        }

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

    def get_jobs(self, type: str) -> List[Dict]:
        """Fetch jobs that need data downloads."""
        cursor = self.db_connection.cursor(dictionary=True)
        if type == 'pending':
            cursor.execute("""
                SELECT * FROM analysis_jobs 
                WHERE status = 'pending'
                ORDER BY created_at
            """)
        elif type == 'queued':
            cursor.execute("""
                SELECT * FROM analysis_jobs 
                WHERE download_status = 'queued'
                ORDER BY created_at
            """)
        elif type == 'ready':
            cursor.execute("""
                SELECT * FROM analysis_jobs 
                WHERE download_status = 'ready'
                ORDER BY created_at
            """)
        else:
            raise ValueError("Invalid job type '{}'".format(type))
        results = cursor.fetchall()
        self.db_connection.commit()
        return results
        
    def get_job_by_id(self, job_id: int) -> Dict:
        """Fetch job by ID."""
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM analysis_jobs WHERE id = %s", (job_id,))
        result = cursor.fetchone()
        self.db_connection.commit()
        return result

    def update_job_download_status(self, job_id: int, status: str, message: str = None) -> None:
        """Update job download status in database."""
        cursor = self.db_connection.cursor()
        if message:
            cursor.execute(
                """
                UPDATE analysis_jobs 
                SET download_status = %s, download_message = %s 
                WHERE id = %s
                """,
                (status, message, job_id)
            )
        else:
            cursor.execute(
                "UPDATE analysis_jobs SET download_status = %s WHERE id = %s",
                (status, job_id)
            )
        self.db_connection.commit()

    def update_job_main_status(self, job_id: int, status: str) -> None:
        """Update job download status in database."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            UPDATE analysis_jobs 
            SET status = %s 
            WHERE id = %s
            """,
            (status, job_id)
        )
        self.db_connection.commit()

    def add_token(self,token_response: str,expiration: datetime) -> None:
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO tokens (headers,expiration) 
            VALUES (%s,%s)
            """,
            (token_response,expiration)
        )
        self.db_connection.commit()

    def check_for_active_token(self) -> Dict:
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM tokens WHERE expiration > NOW()")
        token = cursor.fetchone()
        self.db_connection.commit()
        if token:
            logger.info("Active token found")
            token = json.loads(token['headers'].replace("'",'"'))
            logger.debug("Active token: {}".format(token))
            return token
        else:
            logger.info("No active token found")
            return None

    def open_connection(self) -> Dict:
        """Open connection to AppEEARS and get auth token."""
        logger.info("Opening connection to AppEEARS")
        try:
            token_response = requests.post(
                f"{self.api_url}login",
                auth=(self.config['earthdata_username'], 
                      self.config['earthdata_password'])
            ).json()

            
            logger.debug("Token response received")
            logger.debug(token_response)
            token = token_response['token']
            expiration = datetime.datetime.strptime(token_response['expiration'], '%Y-%m-%dT%H:%M:%SZ')

            # save token to SQL
            self.add_token(str(token_response),expiration)

            return {'Authorization': 'Bearer {}'.format(token)}
        except Exception as e:
            logger.error(f"Error getting auth token: {e}", exc_info=True)
            if token_response:
                logger.error(f"Full token response: {token_response}")
            raise

    def submit_request(self, task_name: str, geometry: Dict, 
                      start_date: str, end_date: str) -> Tuple[str, Dict]:
        """Submit download request to AppEEARS."""
        task = {
            'task_type': 'area',
            'task_name': task_name,
            'params': {
                'dates': [{
                    'startDate': start_date,
                    'endDate': end_date
                }],
                'layers': self.data_defs['MODIS']['productsAndLayers'],
                'output': {
                    'format': {'type': 'geotiff'},
                    'projection': 'geographic'
                },
                'geo': geometry,
            }
        }

        
        task_response = requests.post(
            f"{self.api_url}task",
            json=task,
            headers=self.headers
        ).json()
        
        try:
            task_id = task_response['task_id']
            status_response = requests.get(
                f"{self.api_url}status/{task_id}",
                headers=self.headers
            ).json()
            return task_id, status_response
        except Exception as e:
            logger.error(f"Error submitting task: {e}", exc_info=True)
            logger.error(f"Full response: {task_response}")
            raise

    def download_files(self, job: Dict) -> List[str]:
        try:
            logger.info(f"Downloading files for job {job['id']}")
            job = self.get_job_by_id(job['id']) # Refresh job data
            task_id = job['download_message']

            dest_folder = os.path.join(self.config['download_dir'], f"job_{job['id']}")
            os.makedirs(dest_folder, mode=0o755, exist_ok=True)

            """Download completed task files."""
            bundle = requests.get(
                f"{self.api_url}bundle/{task_id}",
                headers=self.headers
            ).json()
            
            files = {}
            downloaded_files = []
            
            for f in bundle['files']:
                files[f['file_id']] = f['file_name']
            
            for f_id in files:
                dl = requests.get(
                    f"{self.api_url}bundle/{task_id}/{f_id}",
                    headers=self.headers,
                    stream=True,
                    allow_redirects=True
                )
                
                if files[f_id].endswith('.tif'):
                    filename = files[f_id].split('/')[1]
                else:
                    filename = files[f_id]
                
                logger.debug(f"Downloading file: {filename}")
                filepath = os.path.join(dest_folder, f"{task_id}_{filename}")
                
                with open(filepath, 'wb') as f:
                    for data in dl.iter_content(chunk_size=8192):
                        f.write(data)
                
                downloaded_files.append(filepath)
            
            logger.info(f"Finished downloading files for job {job['id']}")
            self.update_job_download_status(job['id'], 'downloaded')
        except Exception as e:
            logger.error(f"Error downloading files for job {job['id']}: {e}", exc_info=True)
            self.update_job_download_status(job['id'], 'failed')#, str(e))
        
    def start_job(self, job: Dict) -> None:
        """Start processing a job."""
        try:
            self.update_job_main_status(job['id'], 'processing')
            
            # Create task name
            task_name = f"ndvi_analysis_job_{job['id']}"
            
            # Create FeatureCollection from geometry
            geometry = json.loads(job['geometry'])
            feature_collection = {
                "type": "FeatureCollection",
                "features": [
                    geometry
                ],
                "fileName": f"analysis_job_{job['id']}"
            }

            logger.debug(f"Feature collection")
            logger.debug(feature_collection)

            # Submit request
            response = self.submit_request(
                task_name=task_name,
                geometry=feature_collection,
                start_date=job['start_date'].strftime('%m-%d-%Y'),#%Y-%m-%d'),
                end_date=job['end_date'].strftime('%m-%d-%Y'),#%Y-%m-%d'),
            )
            
            logger.debug(f"Task submission response: {response}")

            if response[1]['status'] == 'queued':
                self.update_job_main_status(job['id'], 'processing')
                self.update_job_download_status(job['id'], 'queued', message=response[1]['task_id'])
            else:
                logger.error(f"Task submission failed: {response}")
                raise Exception(f"Task submission failed: {response}")
            
        except Exception as e:
            logger.error(f"Error starting job {job['id']}: {str(e)}", exc_info=True)
            self.update_job_main_status(job['id'], 'failed')

    def wait_in_queue(self, job: Dict) -> None:
        """Wait for task to complete."""
        status = requests.get(
            f"{self.api_url}task/{job['download_message']}",
            headers=self.headers
        ).json()['status']
            
        logger.debug(f"Job #{job['id']}, APPEARS Task {job['download_message']}, Status: {status}")
            
        if status == 'done':
            self.update_job_download_status(job['id'], 'ready')
        elif status == 'error':
            self.update_job_main_status(job['id'], 'failed')
            self.update_job_download_status(job['id'], 'failed')
            raise Exception(f"Task {job['download_message']} failed")
        
    def run(self) -> None:
        """Main processing loop."""
        try:
            while True:
                # Start jobs not yet started
                pending_jobs = self.get_jobs('pending')
                if not pending_jobs:
                    logger.info("No pending jobs found, sleeping...")
                    time.sleep(5)
                else:
                    logger.info(f"{len(pending_jobs)} pending jobs found, starting...")
                    for job in pending_jobs:
                        self.start_job(job)
                    time.sleep(5)

                queued_jobs = self.get_jobs('queued')
                if not queued_jobs:
                    logger.info("No queued jobs found, sleeping...")
                    time.sleep(5)
                else:
                    logger.info(f"{len(queued_jobs)} queued jobs found, checking...")
                    for job in queued_jobs:
                        self.wait_in_queue(job)
                    time.sleep(5)

                download_ready_jobs = self.get_jobs('ready')
                if not download_ready_jobs:
                    logger.info("No download-ready jobs found, sleeping...")
                    time.sleep(5)
                else:
                    logger.info(f"{len(download_ready_jobs)} download-ready jobs found, downloading files...")
                    for job in download_ready_jobs:
                        self.download_files(job)
                    time.sleep(5)

        except KeyboardInterrupt:
            logger.info("Shutting down downloader...")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Cleanup resources."""
        if hasattr(self, 'db_connection') and self.db_connection.is_connected():
            self.db_connection.close()

if __name__ == '__main__':
    logger.info("Starting downloader...")
    downloader = MODISDownloader('/var/www/carbon/python/config.json')
    time.sleep(10)  # Wait for database to initialize
    downloader.run()