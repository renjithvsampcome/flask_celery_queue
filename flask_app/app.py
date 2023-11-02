
from flask import Flask, request, jsonify
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from handler import get_access_token, handle_api_insta, handle_api_facebook, handle_twitter_api, get_access_url, get_status, handle_youtube_import
import pangres as pg
import os
import sys
import logging



load_dotenv()

app = Flask(__name__)

app.logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
app.logger.addHandler(stream_handler)

# Redirect sys.stdout and sys.stderr to the Flask logger
sys.stdout = stream_handler.stream
sys.stderr = stream_handler.stream

connection_string = os.environ.get("DB_CONNECTION_STRING")
engine = create_engine(connection_string)

@app.route('/job_status_check/<task_id>')
def get_last_job_status(task_id):
    status = get_status(task_id)
    if status  == "SUCCESS":
        return jsonify({'success': True}), 200
    else:
        return jsonify({"success": False}), 200

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
            last_file_id = None
            if len(response['data'])!= 0:
                result = handle_api_insta(row,response,token,id)
                if result is not None:
                    last_file_id = result
                    
            if response['paging'].get("next"):
                next_url = response['paging']['next']
            else: 
                next_url = None

            
            while next_url:
                res = requests.get(next_url).json()
                if len(res['data'])!= 0:
                    result = handle_api_insta(row,res,token,id)
                    if result is not None:
                        last_file_id = result

                if res['paging'].get("next"):
                    next_url = res['paging']['next']
                else: 
                    next_url = None
            df = pd.DataFrame(row, columns=['file_id', 'media_type', 'media_url',"username","timestamp","is_album","caption"])
            df['user_id'] = id
            #save it to db
            pg.upsert(
                con=engine,
                df=df.set_index('file_id'),
                table_name='import_instagram',
                if_row_exists='update', schema='public', chunksize=10000)
            if last_file_id!=None:
                return jsonify({'status':"Pending", "last_job_id": last_file_id}), 200
            else:
                return jsonify({'status': "No file to upload"}), 201
        else:
            return jsonify({'error':"access token error"}) , 400

    except Exception as e:
        return  jsonify({'error':f"Encounter an exception: {e}"}) , 500
    

@app.route("/get_user_media_fb/<code>/<id>", methods=['GET'])
def import_facebook(code,id):
    try:
        params = {
        'fields': 'id,name,feed{name,status_type,link,permalink_url,sharedposts,created_time,full_picture,is_published,message,type,attachments{media,media_type,url},parent_id}',
        'access_token': code  
        }
        response = requests.get("https://graph.facebook.com/v18.0/me", params=params).json()
    
        row = []
        last_file_id = None
        if len(response['feed']['data'])!=0:
            result = handle_api_facebook(row, response['feed'],id)
            if result is not None:
                    last_file_id = result

        if 'paging' in response and response['paging'].get("next"):
                    next_url = response['paging']['next']
        else: 
            next_url = None

        while next_url:
            res = requests.get(next_url).json()
            if len(res['data'])!= 0:
                result = handle_api_facebook(row,res,id)
                if result is not None:
                    last_file_id = result

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
        return jsonify({'status':"Pending", "last_job_id": last_file_id}), 200
        
    except Exception as e:
        return  jsonify({'error':f"Encounter an exception importing facebook data: {e}"}), 500

@app.route("/get_user_media_url_twitter/",methods=['GET'])
def get_url():
    return get_access_url()

@app.route("/get_user_media_twitter/<ot>/<ots>/<verifier>/<id>/", methods=['GET'])
def import_twitter(ot, ots, verifier, id):
    try:
        row = []
        row , r = handle_twitter_api(row, ot, ots, verifier,id)
        df = pd.DataFrame(row, columns=['file_id', 'media_type', 'media_url',"username"])
        df['user_id'] = id
        if len(row)!= 0:
            pg.upsert(
                        con=engine,
                        df=df.set_index('file_id'),
                        table_name='import_twitter',
                        if_row_exists='update', schema='public', chunksize=10000)
            return jsonify({'status':"Pending", "last_job_id": r}) , 200
        else:
            return jsonify({'error': "No data to upload"}) , 400
    except Exception as e:
        return  jsonify({'error':f"Encounter an exception importing twitter data: {e}"}),500

@app.route("/get_user_media_youtube/<channel_id>/<id>/",methods=['GET'])
def import_youtube(channel_id,id):
    try:
        data = []
        row , last_file_id = handle_youtube_import(data,channel_id,id)
        if len(row)==0:
            return jsonify({'error': "No shorts to upload"}) , 400
        else:
            df = pd.DataFrame(row, columns=['file_id', 'title', 'published_time',"file_url","type"])
            df['user_id'] = id
            df['channel_id'] = channel_id
            pg.upsert(
                        con=engine,
                        df=df.set_index('file_id'),
                        table_name='import_youtube',
                        if_row_exists='update', schema='public', chunksize=10000)
            return jsonify({'status':"Pending", "last_job_id": last_file_id}) , 200
    except Exception as e:
        return  jsonify({'error':f"Encounter an exception importing youtube data: {e}"}),500


