"""
Web results uploader for satellite analysis data.
Prepares analysis results for web visualization.
"""

import os
import json
import datetime, time
import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, List
import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(
    filename=f'/var/log/carbon/satellite_uploader_{datetime.datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ResultsUploader:
    def __init__(self, config_path: str):
        """Initialize the uploader with configuration settings."""
        self.config = self._load_config(config_path)
        self.db_connection = self._init_db_connection()
        self.debug = self.config.get('debug', False)
        if self.debug:
            logger.info("Debug mode activated")
            logger.setLevel(logging.DEBUG)
        
        # Create web results directory
        self.web_dir = self.config['web_results_dir']
        os.makedirs(self.web_dir, exist_ok=True)

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path) as f:
            return json.load(f)

    def _init_db_connection(self) -> mysql.connector.connection.MySQLConnection:
        try:
            return mysql.connector.connect(**self.config['db'])
        except Error as e:
            logger.error(f"Database connection error: {e}", exc_info=True)
            raise

    def get_completed_jobs(self) -> List[Dict]:
        """Fetch jobs that need web results preparation."""
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT aj.*, jr.* 
            FROM analysis_jobs aj
            JOIN job_results jr ON aj.id = jr.job_id
            WHERE aj.analysis_status = 'completed' 
            AND (aj.web_status IS NULL OR aj.web_status = 'pending')
            ORDER BY aj.created_at
        """)
        results = cursor.fetchall()
        self.db_connection.commit()
        return results
    

    def update_job_status(self, job_id: int, status: str, message: str = None, level: str = None) -> None:
        """Update job web preparation status."""
        cursor = self.db_connection.cursor()
        if level is None:
            query = """
                UPDATE analysis_jobs 
                SET web_status = %s, web_message = %s 
                WHERE id = %s
            """ if message else """
                UPDATE analysis_jobs 
                SET web_status = %s 
                WHERE id = %s
            """
            params = (status, message, job_id) if message else (status, job_id)
        elif level == "main":
            query = """
                UPDATE analysis_jobs 
                SET status = %s 
                WHERE id = %s
            """
            params = (status, job_id)
        else:
            raise ValueError(f"Invalid status level: {level}")
            
        cursor.execute(query, params)
        self.db_connection.commit()

    def prepare_visualization_data(self, job: Dict) -> Dict:
        """Prepare data for web visualization."""
        logger.debug("Preparing visualization data for job %s", job['job_id'])
        timeseries = json.loads(job['timeseries_data'])
        
        # Convert to pandas for easier manipulation
        df = pd.DataFrame(timeseries)
        df['date'] = pd.to_datetime(df['date'])
        
        # Create visualization format
        viz_data = {
            'timeseries': {
                'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'mean_ndvi': df['mean_ndvi'].tolist(),
                'min_ndvi': df['min_ndvi'].tolist(),
                'max_ndvi': df['max_ndvi'].tolist()
            },
            'summary': {
                'overall_change': job['overall_change'],
                'trend_annual': job['trend'],
                'max_observed': job['max_ndvi'],
                'min_observed': job['min_ndvi'],
                'mean_variability': job['mean_std']
            },
            'metadata': {
                'job_id': job['job_id'],
                'analysis_date': job['updated_at'].strftime('%Y-%m-%d'),
                'study_period': {
                    'start': df['date'].min().strftime('%Y-%m-%d'),
                    'end': df['date'].max().strftime('%Y-%m-%d')
                },
                'geometry': json.loads(job['geometry'])
            }
        }
        
        return viz_data

    def create_downloads(self, job: Dict, viz_data: Dict) -> None:
        logger.debug("Creating downloads for job %s", job['job_id'])
        """Create downloadable files for the web interface."""
        job_web_dir = os.path.join(self.web_dir, f"job_{job['job_id']}")
        os.makedirs(job_web_dir, exist_ok=True)
        
        # Save JSON results
        with open(os.path.join(job_web_dir, 'results.json'), 'w') as f:
            json.dump(viz_data, f, indent=2)
        
        # Create CSV of timeseries data
        df = pd.DataFrame({
            'date': viz_data['timeseries']['dates'],
            'mean_ndvi': viz_data['timeseries']['mean_ndvi'],
            'min_ndvi': viz_data['timeseries']['min_ndvi'],
            'max_ndvi': viz_data['timeseries']['max_ndvi']
        })
        df.to_csv(os.path.join(job_web_dir, 'timeseries.csv'), index=False)

    def process_job(self, job: Dict) -> None:
        logger.info("Processing results for job %s", job['job_id'])
        """Process a single job's results for web visualization."""
        #logger.debug("Full job details: %s", job)
        try:
            self.update_job_status(job['job_id'], 'preparing')
            
            # Prepare visualization data
            viz_data = self.prepare_visualization_data(job)
            
            # Create downloadable files
            self.create_downloads(job, viz_data)
            
            # Update web interface data
            self.update_web_interface(job['job_id'], viz_data)
            
            # email people
            email_status = self.send_completion_email(job['job_id'], job['email'])

            # Update status based on email success
            if email_status:
                self.update_job_status(
                    job['job_id'], 
                    'completed', 
                    "Web results prepared successfully and notification sent",
                )
            else:
                self.update_job_status(
                    job['job_id'], 
                    'completed', 
                    "Web results prepared successfully but notification failed",
                )

            self.update_job_status(
                job['job_id'], 
                'completed',
                level="main"
            )

            logger.info("Job %s fully processed!", job['job_id'])
            
        except Exception as e:
            logger.error(f"Error preparing web results for job {job['job_id']}: {str(e)}", exc_info=True)
            self.update_job_status(job['job_id'], 'failed', str(e))

    def send_completion_email(self, job_id: int, email: str) -> bool:
        """Send completion notification email to user."""
        logger.debug(f"Sending completion email for job {job_id} to {email}")
        
        try:
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f'Your Satellite Analysis Results Are Ready (Job #{job_id})'
            msg['From'] = 'noreply@jariz.dev'
            msg['To'] = email

            # Create HTML content
            html = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <h2 style="color: #047857;">Your Analysis Results Are Ready</h2>
                <p>Your satellite imagery analysis job (ID: {job_id}) has been completed and is ready for viewing.</p>
                <p>You can view your results at: <a href="https://gis.jariz.dev/results.php?id={job_id}">View Results</a></p>
                <p>The results include:</p>
                <ul>
                    <li>Time series analysis of vegetation changes</li>
                    <li>Statistical summaries</li>
                    <li>Downloadable data</li>
                    <li>Interactive map visualization</li>
                </ul>
                <p style="margin-top: 20px;">If you have any questions, please don't hesitate to contact us.</p>
            </body>
            </html>
            """

            msg.attach(MIMEText(html, 'html'))

            # Send using local Postfix
            import smtplib
            with smtplib.SMTP('localhost') as server:
                server.send_message(msg)
                
            logger.info(f"Successfully sent completion email for job {job_id} to {email}")
            return True

        except Exception as e:
            logger.error(f"Failed to send completion email for job {job_id}: {str(e)}", exc_info=True)
            return False

    def update_web_interface(self, job_id: int, viz_data: Dict) -> None:
        logger.debug("Updating web interface for job %s", job_id)
        try:
            """Update the web interface with new visualization data."""
            cursor = self.db_connection.cursor()
            
            cursor.execute("""
                INSERT INTO web_visualizations 
                    (job_id, viz_data, chart_config)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    viz_data = VALUES(viz_data),
                    chart_config = VALUES(chart_config)
            """, (
                job_id,
                json.dumps(viz_data),
                json.dumps(self._generate_chart_config(viz_data))
            ))
            
            self.db_connection.commit()
        except Exception as e:
            logger.error(f"Error updating web interface for job {job_id}: {str(e)}", exc_info=True)
            logger.error(f"Visualization data: {viz_data}")
            raise

    def _generate_chart_config(self, viz_data: Dict) -> Dict:
        """Generate configuration for web visualizations."""
        return {
            'timeseries': {
                'type': 'line',
                'data': {
                    'labels': viz_data['timeseries']['dates'],
                    'datasets': [
                        {
                            'label': 'Mean NDVI',
                            'data': viz_data['timeseries']['mean_ndvi'],
                            'fill': False,
                            'borderColor': 'rgb(75, 192, 192)'
                        },
                        {
                            'label': 'Min/Max Range',
                            'data': list(zip(
                                viz_data['timeseries']['min_ndvi'],
                                viz_data['timeseries']['max_ndvi']
                            )),
                            'type': 'area',
                            'backgroundColor': 'rgba(75, 192, 192, 0.2)'
                        }
                    ]
                },
                'options': {
                    'responsive': True,
                    'scales': {
                        'y': {'title': {'text': 'NDVI'}},
                        'x': {'title': {'text': 'Date'}}
                    }
                }
            }
        }

    def run(self) -> None:
        """Main processing loop."""
        try:
            while True:
                pending_jobs = self.get_completed_jobs()
                if not pending_jobs:
                    logger.info("No pending jobs found, sleeping...")
                
                for job in pending_jobs:
                    self.process_job(job)

                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("Shutting down uploader...")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Cleanup resources."""
        if hasattr(self, 'db_connection') and self.db_connection.is_connected():
            self.db_connection.close()

if __name__ == '__main__':
    uploader = ResultsUploader('/var/www/carbon/python/config.json')
    logger.info("Starting uploader...")
    time.sleep(10)  # Wait for database to initialize
    uploader.run()