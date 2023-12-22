import time
import requests
import json
import boto3
import os
from celery import Celery
from celery.utils.log import get_task_logger
from pytube import YouTube
from pytube.exceptions import VideoUnavailable
import psycopg2
import shutil
from test_auth_of import test_login
from bs4 import BeautifulSoup


logger = get_task_logger(__name__)

bucket_endpoint = os.environ.get("BUCKET_ENDPOINT")
bucket_access = os.environ.get("BUCKET_ACCESS")
bucket_secret = os.environ.get("BUCKET_SECRET")
bucket_name = os.environ.get("BUCKET_NAME")

app = Celery(
    "tasks", broker=os.environ.get("REDIS_URL"), backend=os.environ.get("REDIS_URL")
)


s3 = boto3.client(
    "s3",
    endpoint_url=bucket_endpoint,
    aws_access_key_id=bucket_access,
    aws_secret_access_key=bucket_secret,
)

db_connection = None


def get_db_connection():
    global db_connection
    if db_connection is None:
        db_connection = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            database=os.environ.get("DB_DATABASE"),
        )
    return db_connection


@app.task()
def handle_file(url, name, type):
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        # Send an HTTP GET request to the URL
        response = requests.get(url, stream=True, timeout=15)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Open a local file for writing the video and image data
            if type == "VIDEO":
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
            s3.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=file_name)
            os.remove(file_name)
            logger.info("File saved successfully.")
            file_url = (
                f"https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}"
            )
            job_id = handle_file.request.id
            update_query = (
                "INSERT INTO public.import_job_status (job_id, status) VALUES (%s, %s)"
            )
            cursor.execute(update_query, (job_id, "SUCCESS"))
            cursor.close()
            db_conn.commit()
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
        return None


@app.task()
def handle_youtube_file(url, name, type):
    try:
        yt = YouTube(url)
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        # print(f'Downloading video: {url}')
        file_name = f"shorts_{name}.mp4"
        yt.streams.get_highest_resolution().download(
            output_path=".", filename=file_name
        )
        s3.upload_file(file_name, bucket_name, file_name)
        s3.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=file_name)
        os.remove(file_name)
        logger.info("File saved successfully.")
        file_url = (
            f"https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}"
        )
        # add to dv
        job_id = handle_youtube_file.request.id
        update_query = (
            "INSERT INTO public.import_job_status (job_id, status) VALUES (%s, %s)"
        )
        cursor.execute(update_query, (job_id, "SUCCESS"))
        cursor.close()
        db_conn.commit()
        return file_url
    except:
        raise Exception(f"Video is unavaialable, skipping.")


@app.task()
def handle_login_onlyfans(userid, email, pwd):
    handle_login = test_login(email, pwd, userid)
    folder_path = "profiles"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_path}' and its contents have been removed.")
    else:
        print(f"Folder '{folder_path}' does not exist.")
        raise Exception("failed")
    if handle_login == False:
        raise Exception("Task Failed while login_onlyfans")

@app.task()
def handle_tiktok_task(url, file_name):
    try:
        headers = {
            'authority': 'www.tiktok.com',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        session = requests.Session()
        response = session.get(url, headers=headers)
        if response.status_code >= 400:
            raise Exception(f"Error : getting tiktok session {response.status_code}")
        # soup = BeautifulSoup(response.content, 'html.parser')
        # script = soup.select_one('script#__UNIVERSAL_DATA_FOR_REHYDRATION__')
        
        headers = {
            'authority': 'v16-webapp-prime.tiktok.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'origin': 'https://www.tiktok.com',
            'pragma': 'no-cache',
            'range': 'bytes=0-',
            'referer': 'https://www.tiktok.com/',
            'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'video',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            }
        # download(videoUrl, './video/'+author+'-'+createTime+'.'+'.mp4', videoFormat, headers, session)
        session.headers.update(headers)
        response = session.get(url, headers=headers, stream=True)
        response.raise_for_status()  # Check if the request was successful
        with open('./'+ file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        s3.upload_file(file_name, bucket_name, file_name)
        s3.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=file_name)
        os.remove(file_name)
        logger.info("File saved successfully.")
        return None  
    except:
        raise Exception(f"Video is unavaialable, skipping.")