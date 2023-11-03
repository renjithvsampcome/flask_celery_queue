import time
import requests
import boto3
import os
from celery import Celery
from celery.utils.log import get_task_logger
from pytube import YouTube
from pytube.exceptions import VideoUnavailable
import psycopg_binary

logger = get_task_logger(__name__)

bucket_endpoint = os.environ.get("BUCKET_ENDPOINT")
bucket_access = os.environ.get("BUCKET_ACCESS") 
bucket_secret = os.environ.get("BUCKET_SECRET")
bucket_name = os.environ.get("BUCKET_NAME")

app = Celery('tasks', broker=os.environ.get('REDIS_URL'), backend=os.environ.get('REDIS_URL'))


s3 = boto3.client('s3',
                endpoint_url=bucket_endpoint,
                aws_access_key_id=bucket_access,
                aws_secret_access_key=bucket_secret)

db_connection = None

def get_db_connection():
    global db_connection
    if db_connection is None:
        db_connection = psycopg_binary.connect(
            host=os.environ.get('DB_HOST'),
            port=os.environ.get("DB_PORT"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            database=os.environ.get("DB_DATABASE")
        )
    return db_connection



@app.task()
def handle_file(url,name,type):
    try:
        cursor = db_connection.cursor()
        # Send an HTTP GET request to the URL
        response = requests.get(url, stream=True, timeout=15)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Open a local file for writing the video and image data
            if type == 'VIDEO':
                file_name = f"video_{name}.mp4"
            elif type == "IMAGE":
                file_name = f"image_{name}.jpg"
            else:
                return None
            with open(file_name, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            
            s3.upload_file(file_name, bucket_name, file_name)
            s3.put_object_acl(ACL='public-read', Bucket=bucket_name, Key=file_name)
            os.remove(file_name)
            logger.info("File saved successfully.")
            file_url = f'https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}'
            job_id = handle_file.request.id
            update_query = "INSERT INTO public.import_job_status (job_id, status) VALUES (%s, %s)"
            cursor.execute(update_query, (job_id,"SUCCESS"))
            cursor.close()
            db_connection.commit()
            return file_url
        else:
            logger.info(f"Request failed with status code {response.status_code}")
            cursor.close()
            return None
    except requests.exceptions.Timeout:
        logger.info("Request timed out. Increase the timeout value.")
        cursor.close()
        return None

    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")
        db_connection.close()
        return None

@app.task()
def handle_youtube_file(url,name,type):
    try:
        yt = YouTube(url)
        cursor = db_connection.cursor()
        # print(f'Downloading video: {url}')
        file_name = f"shorts_{name}.mp4"
        yt.streams.first().download(output_path='.', filename=file_name)
        s3.upload_file(file_name, bucket_name, file_name)
        s3.put_object_acl(ACL='public-read', Bucket=bucket_name, Key=file_name)
        os.remove(file_name)
        logger.info("File saved successfully.")
        file_url = f'https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}'
        # add to dv
        job_id = handle_file.request.id
        update_query = "INSERT INTO public.import_job_status (job_id, status) VALUES (%s, %s)"
        cursor.execute(update_query, (job_id,"SUCCESS"))
        cursor.close()
        db_connection.commit()
        return file_url
    except VideoUnavailable:
        print(f'Video {url} is unavaialable, skipping.')
        db_connection.close()
    else:
        return None
        
        
        