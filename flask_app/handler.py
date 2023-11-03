from flask import jsonify
import requests
import boto3
import os
from dotenv import load_dotenv
from celery import Celery
import tweepy
from celery.utils.log import get_task_logger
import googleapiclient.discovery
import json
from pytube import YouTube

load_dotenv()
logger = get_task_logger(__name__)

simple_app = Celery('simple_worker', broker=os.environ.get('REDIS_URL'), backend=os.environ.get('REDIS_URL'))

API_KEY_YOUTUBE = os.environ.get("API_KEY_YOUTUBE")
client_id_dev = os.environ.get("CLIENT_ID_DEV")
client_secret_dev = os.environ.get("CLINET_SECERET_DEV")
redirect_uri_dev = os.environ.get("REDIRECT_URI_DEV")

consumer_key_twitter = os.environ.get("TWITTER_CONSUMER_KEY")
consumer_secret_twitter = os.environ.get("TWITTER_CONSUMER_SECRET")


def get_status(task_id):
    status = simple_app.AsyncResult(task_id, app=simple_app)
    return str(status.state)


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
    elif type == "SHORTS":
        file_name = f"shorts_{name}.mp4"
    else:
        return None
    return f'https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{file_name}'

def handle_api_insta(row,response, token,idd):
    r = None
    for data in response['data']:
        caption = data['caption'] if 'caption' in data else None
        if data['media_type'] == 'VIDEO' or data['media_type'] == 'IMAGE':
            file_url = give_file_name(f"{data['id']}{idd}", data['media_type'])
            if file_url:
                r = simple_app.send_task('tasks.handle_file', kwargs={'url': data['media_url'], 'name': f"{data['id']}{idd}", 'type': data['media_type']})
                # file_url = handle_file(data['media_url'], data['id'], data['media_type'])
                row.append((f"{data['id']}{idd}", data["media_type"],file_url ,data["username"],data['timestamp'],False, caption, r.id))
                
        elif data['media_type'] == 'CAROUSEL_ALBUM':
            params = {
                'fields': 'id,media_type,media_url,username,timestamp',
                'access_token': token['access_token']
            }
            res = requests.get(f"https://graph.instagram.com/{data['id']}/children", params=params).json()
            count=0
            for d in res['data']:
                file_url = give_file_name(f"{d['id']}{idd}{count}", d['media_type'])
                if file_url:
                    r = simple_app.send_task('tasks.handle_file', kwargs={'url': d['media_url'], 'name': f"{d['id']}{idd}{count}", 'type': d['media_type']})
                    row.append((f"{data['id']}{idd}{count}",d["media_type"],file_url ,d["username"],d['timestamp'],True,caption,r.id))  
                    count+=1
        else:
            pass
    if r != None:
        return r.id
    else:
        return None


def handle_api_facebook(row,response,idd):
    r = None
    for data in response['data']:
        if data['status_type'] == "added_photos" or data['status_type'] == "added_video":
            message = data['message'] if 'message' in data else None
            if data['type'] == 'photo':
                data['type'] = "IMAGE"
                file_url = give_file_name(f"{data['id']}{idd}", data['type'])
                if file_url:
                    r = simple_app.send_task('tasks.handle_file', kwargs={'url': data['full_picture'], 'name': f"{data['id']}{idd}", 'type': data['type'] })
                    row.append((f"{data['id']}{idd}", "IMAGE", file_url ,"username",data['created_time'],False, message, r.id))
                
            elif data['type'] == "video":
                data['type'] = 'VIDEO'
                if 'source' in data['attachments']['data'][0]['media']:
                    file_url = give_file_name(f"{data['id']}{idd}", data['type'] )
                    if file_url:
                        r = simple_app.send_task('tasks.handle_file', kwargs={'url': data['attachments']['data'][0]['media']['source'], 'name': f"{data['id']}{idd}", 'type': data['type'] })
                        row.append((f"{data['id']}{idd}", "VIDEO", file_url ,"username",data['created_time'],False, message, r.id))
                        
            else: 
                pass
        else:
            pass
    if r != None:
        return r.id
    else:
        return None

#twitter

def get_access_url():
    try:    
        oauth1_user_handler = tweepy.OAuth1UserHandler(
            consumer_key_twitter, consumer_secret_twitter,
            callback=os.environ.get("TWITTER_CALLBACK")
        )
        return jsonify({'result': f"{oauth1_user_handler.get_authorization_url()}",'request_token':f"{oauth1_user_handler.request_token['oauth_token']}",'request_secret':f"{oauth1_user_handler.request_token['oauth_token_secret']}"}) , 200
    except Exception as e:
        return jsonify({'error': f"{e}"}), 400

def handle_twitter_api(row, ot, ots, verifier,idd):
    try:
        new_oauth1_user_handler = tweepy.OAuth1UserHandler(
            consumer_key_twitter, consumer_secret_twitter,
            callback=os.environ.get("TWITTER_CALLBACK")
        )
        new_oauth1_user_handler.request_token = {
            "oauth_token": ot,
            "oauth_token_secret": ots
        }
        access_token, access_token_secret = new_oauth1_user_handler.get_access_token(verifier)
        client = tweepy.Client(consumer_key=consumer_key_twitter,consumer_secret=consumer_secret_twitter,access_token=access_token,access_token_secret=access_token_secret)
        # client.get_home_timeline()
        user = client.get_me()

        data = client.get_users_tweets(user.data.id,user_auth=True,tweet_fields = ['created_at', 'text', 'id', 'attachments','author_id', 'entities'], media_fields=["url","type","media_key","preview_image_url"], expansions=['attachments.media_keys', 'author_id'])
        
        r = None
        for data in data.includes['media']:
            if data.type == 'photo':
                data.type = "IMAGE"
                file_url = give_file_name(f"{data.media_key}{idd}", data.type)
                if file_url:
                    r = simple_app.send_task('tasks.handle_file', kwargs={'url': data.url, 'name': f"{data.media_key}{idd}", 'type': data.type })
                    row.append((f"{data.media_key}{idd}", data.type,file_url,user.data.username, r.id ))
            elif data.type == "video":
                data.type = "IMAGE"
                file_url = give_file_name(f"{data.media_key}{idd}", data.type)
                if file_url:
                    r = simple_app.send_task('tasks.handle_file', kwargs={'url': data.preview_image_url, 'name': f"{data.media_key}{idd}", 'type': data.type })
                    row.append((f"{data.media_key}{idd}", data.type, file_url, user.data.username, r.id))
            else:
                pass
        return row , r.id
    except Exception as e:
        logger.info(f"Encounter an exception: {e}")
        return  [] , None
        
    # print(data.includes['media'][0].preview_image_url) 
    # print(data.includes['media'][0].ype)



def get_channel_video(channel_id):
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY_YOUTUBE)

    videos = []
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    # print(res)
    playlist_id =res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    while True:
        result = youtube.playlistItems().list(playlistId=playlist_id, part = 'snippet',maxResults = 50, pageToken = next_page_token).execute()
        videos += result['items']

        if 'nextPageToken' not in result:
            break
        next_page_token = res['nextPageToken']
        if next_page_token is None:
            break
    return videos

def handle_youtube_import(row, channel_id,id):
    videos = get_channel_video(channel_id)
    print(videos)
    r = None
    if len(videos) != 0:
        for d in videos:
            video_id = d['snippet']['resourceId']['videoId']
            video_url = f"https://www.youtube.com/shorts/{video_id}"
            try:
                yt = YouTube(video_url)
                if yt.length <= 100:
                    name = f"{video_id}{id}"
                    file_url = give_file_name(name,'SHORTS')
                    if file_url:
                        r = simple_app.send_task('tasks.handle_youtube_file', kwargs={'url': video_url, 'name': name})
                        row.append((name, d['snippet']['title'], d['snippet']['publishedAt'], file_url,"SHORTS", r.id))
            except:
                pass
                
            # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            # response = requests.head(video_url,allow_redirects=True, headers=headers)
            # print(video_url)
            # print(response.status_code)
            # print(response.url)
            # if response.url == video_url:
            # if response.status_code == 200:
            #     name = f"{video_id}{id}"
            #     file_url = give_file_name(name,'SHORTS')
            #     if file_url:
            #         r = simple_app.send_task('tasks.handle_youtube_file', kwargs={'url': video_url, 'name': name , 'type': "SHORTS" })
            #         row.append((name, d['snippet']['title'], d['snippet']['publishedAt'], file_url,"SHORTS"))
        if r is not None:
            return row, r.id
        else:
            return row, None
        
    else:
        return row, None

    
