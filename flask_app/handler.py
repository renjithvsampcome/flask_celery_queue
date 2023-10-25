from flask import jsonify
import requests
import boto3
import os
from dotenv import load_dotenv
from celery import Celery
import tweepy
from celery.utils.log import get_task_logger
import json


load_dotenv()
logger = get_task_logger(__name__)

simple_app = Celery('simple_worker', broker='redis://redis:6379/0', backend='redis://redis:6379/0')

client_id_dev = os.environ.get("CLIENT_ID_DEV")
client_secret_dev = os.environ.get("CLINET_SECERET_DEV")
redirect_uri_dev = os.environ.get("REDIRECT_URI_DEV")

consumer_key_twitter = os.environ.get("TWITTER_CONSUMER_KEY")
consumer_secret_twitter = os.environ.get("TWITTER_CONSUMER_SECRET")


def get_access_token(code):

    # Form data for the POST request
    data = {
        'client_id': client_id_dev,
        'client_secret': client_secret_dev,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri_dev,
        'code': code
    }

    try:
        # Make the POST request to Instagram
        response = requests.post("https://api.instagram.com/oauth/access_token", data=data)
        return response.json()
    
    except Exception as e:
        return jsonify({'error': str(e)})

def give_file_name(name,type):
    if type == 'VIDEO':
        file_name = f"video_{name}.mp4"
    elif type == "IMAGE":
        file_name = f"image_{name}.jpg"
    else:
        return None
    return f'https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}'

def handle_api_insta(row,response, token):
    for data in response['data']:
        caption = data['caption'] if 'caption' in data else None
        if data['media_type'] == 'VIDEO' or data['media_type'] == 'IMAGE':
            simple_app.send_task('tasks.handle_file', kwargs={'url': data['media_url'], 'name': data['id'], 'type': data['media_type']})
            file_url = give_file_name(data['id'], data['media_type'])
            # file_url = handle_file(data['media_url'], data['id'], data['media_type'])
            row.append((data['id'], data["media_type"],file_url ,data["username"],data['timestamp'],False, caption))
        elif data['media_type'] == 'CAROUSEL_ALBUM':
            params = {
                'fields': 'id,media_type,media_url,username,timestamp',
                'access_token': token['access_token']
            }
            res = requests.get(f"https://graph.instagram.com/{data['id']}/children", params=params).json()
            for d in res['data']:
                simple_app.send_task('tasks.handle_file', kwargs={'url': data['media_url'], 'name': data['id'], 'type': data['media_type']})
                file_url = give_file_name(data['id'], data['media_type'])
                row.append((d['id'],d["media_type"],file_url ,d["username"],d['timestamp'],True,caption))
        else:
            pass


def handle_api_facebook(row,response):
    for data in response['data']:
        if data['status_type'] == "added_photos" or data['status_type'] == "added_video":
            message = data['message'] if 'message' in data else None
            if data['type'] == 'photo':
                data['type'] = "IMAGE"
                simple_app.send_task('tasks.handle_file', kwargs={'url': data['full_picture'], 'name': data['id'], 'type': data['type'] })
                file_url = give_file_name(data['id'], data['type'])
                row.append((data['id'], "IMAGE", file_url ,"username",data['created_time'],False, message))
            elif data['type'] == "video":
                data['type'] = 'VIDEO'
                if 'source' in data['attachments']['data'][0]['media']:
                    simple_app.send_task('tasks.handle_file', kwargs={'url': data['attachments']['data'][0]['media']['source'], 'name': data['id'], 'type': data['type'] })
                    file_url = give_file_name(data['id'], data['type'] )
                    row.append((data['id'], "VIDEO", file_url ,"username",data['created_time'],False, message))
                else:
                    pass
            else: 
                pass
        else:
            pass

#twitter
oauth1_user_handler = tweepy.OAuth1UserHandler(
    consumer_key_twitter, consumer_secret_twitter,
    callback=os.environ.get("TWITTER_CALLBACK")
)

def get_access_url():
    return oauth1_user_handler.get_authorization_url(signin_with_twitter=True)

def handle_twitter_api(row, code):
    try:
        access_token, access_token_secret = oauth1_user_handler.get_access_token(code)
        client = tweepy.Client(consumer_key=consumer_key_twitter,consumer_secret=consumer_secret_twitter,access_token=access_token,access_token_secret=access_token_secret)
        # client.get_home_timeline()
        user = client.get_me()

        data = client.get_users_tweets(user.data.id,user_auth=True,tweet_fields = ['created_at', 'text', 'id', 'attachments','author_id', 'entities'], media_fields=["url","type","media_key","preview_image_url"], expansions=['attachments.media_keys', 'author_id'])
        
        for data in data.includes['media']:
            if data.type == 'photo':
                data.type = "IMAGE"
                file_url = give_file_name(data.media_key, data.type )
                simple_app.send_task('tasks.handle_file', kwargs={'url': data.url, 'name': data.media_key, 'type': data.type })
                row.append((data.media_key, data.type,file_url,user.data.username ))
            elif data.type == "video":
                data.type = "IMAGE"
                file_url = give_file_name(data.media_key, data.type)
                simple_app.send_task('tasks.handle_file', kwargs={'url': data.preview_image_url, 'name': data.media_key, 'type': data.type })
                row.append((data.media_key, data.type, file_url, user.data.username))
            else:
                pass
        return row
    except Exception as e:
        logger.info(f"Encounter an exception: {e}")
        return  []
        
    # print(data.includes['media'][0].preview_image_url) 
    # print(data.includes['media'][0].ype)
