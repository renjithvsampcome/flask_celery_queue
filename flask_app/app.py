
from celery import Celery
from flask import Flask, request, jsonify
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from handler import get_access_token, handle_api_insta, handle_api_facebook, handle_twitter_api, get_access_url
import pangres as pg
import os
import logging
from logging.handlers import RotatingFileHandler
import tweepy

# Configure logging
log_handler = RotatingFileHandler('app.log', maxBytes=10240, backupCount=10)
log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(log_handler)
root_logger.addHandler(console_handler)

load_dotenv()

app = Flask(__name__)



connection_string = os.environ.get("DB_CONNECTION_STRING")
engine = create_engine(connection_string)

@app.route('/get_user_media_insta/<code>/<id>', methods=['GET'])
def import_instagram(code,id):

    token = get_access_token(code)
    app.logger.debug(token)

    try:
        if 'access_token' in token:
            #query parameters

            params = {
                'fields': 'id,caption,media_type,media_url,timestamp,username',
                'access_token': token['access_token']
            }
            # Make the GET request
            response = requests.get("https://graph.instagram.com/me/media", params=params).json()
            # row tp get data of each post
            row = []
            if len(response['data'])!= 0:
                handle_api_insta(row,response,token)
                

            if response['paging'].get("next"):
                next_url = response['paging']['next']
            else: 
                next_url = None

            while next_url:
                res = requests.get(next_url).json()
                if len(res['data'])!= 0:
                    handle_api_insta(row,res,token)

                if res['paging'].get("next"):
                    next_url = res['paging']['next']
                else: 
                    next_url = None
            df = pd.DataFrame(row, columns=['file_id', 'media_type', 'media_url',"username","timestamp","is_album","caption"])
            df['user_id'] = id
            #save it to db
            df.to_csv("myfile.csv")
            pg.upsert(
                con=engine,
                df=df.set_index('file_id'),
                table_name='import_instagram',
                if_row_exists='update', schema='public', chunksize=10000)

            return jsonify({'result':"Data upload is successfull"})
        else:
            return jsonify({'error':"access token error"})

    except Exception as e:
        return  jsonify({'error':f"Encounter an exception: {e}"})
    

@app.route("/get_user_media_fb/<code>/<id>", methods=['GET'])
def import_facebook(code,id):
    try:
        params = {
        'fields': 'id,name,feed{name,status_type,link,permalink_url,sharedposts,created_time,full_picture,is_published,message,type,attachments{media,media_type,url},parent_id}',
        'access_token': code  # Replace 'YOUR_ACCESS_TOKEN' with your actual access token
        }
        response = requests.get("https://graph.facebook.com/v18.0/me", params=params).json()
    
        row = []

        if len(response['feed']['data'])!=0:
            handle_api_facebook(row, response['feed'])

        if 'paging' in response and response['paging'].get("next"):
                    next_url = response['paging']['next']
        else: 
            next_url = None

        while next_url:
            res = requests.get(next_url).json()
            if len(res['data'])!= 0:
                handle_api_facebook(row,res)

            if 'paging' in res and res['paging'].get("next"):
                    next_url = res['paging']['next']
            
            else: 
                next_url = None
        df = pd.DataFrame(row, columns=['file_id', 'media_type', 'media_url',"username","timestamp","is_album","caption"])
        df['username'] = response['name']
        df['user_id'] = id
        pg.upsert(
                con=engine,
                df=df.set_index('file_id'),
                table_name='import_facebook',
                if_row_exists='update', schema='public', chunksize=10000)
        return jsonify({'result':"FB Data upload is successfull"})
        
    except Exception as e:
        return  jsonify({'error':f"Encounter an exception importing facebook data: {e}"})

@app.route("/get_user_media_url/",methods=['GET'])
def get_url():
    return get_access_url()

@app.route("/get_user_media_twitter/<code>/<id>/", methods=['GET'])
def import_twitter(code,id):
    row = []
    row = handle_twitter_api(row, code)
    df = pd.DataFrame(row, columns=['file_id', 'media_type', 'media_url',"username"])
    df['user_id'] = id
    if len(row)!= 0:
        pg.upsert(
                    con=engine,
                    df=df.set_index('file_id'),
                    table_name='import_twitter',
                    if_row_exists='update', schema='public', chunksize=10000)
        return jsonify({'result':"twitter Data upload is successfull"})
    else:
        return jsonify({'error': "No data to upload"})



