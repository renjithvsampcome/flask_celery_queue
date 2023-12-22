import re
import os
import sys
import json
import shutil
import requests
import time
import datetime as dt
import hashlib
import pandas as pd
import random
import boto3
from sqlalchemy import create_engine
import pangres as pg
from dotenv import load_dotenv

load_dotenv()

# maximum number of posts to index
# DONT CHANGE THAT
POST_LIMIT = 100

# api info
URL = "https://onlyfans.com"
API_URL = "/api2/v2"

# \TODO dynamically get app token
# Note: this is not an auth token
APP_TOKEN = "33d57ade8c02dbc5a333db99ff9ae26a"

# user info from /users/customer
USER_INFO = {}

# target profile
PROFILE = ""
# profile data from /users/<profile>
PROFILE_INFO = {}
PROFILE_ID = ""


# helper function to make sure a dir is present
def assure_dir(path):
    if not os.path.isdir(path):
        os.mkdir(path)


# Create Auth with Json
def create_auth(ljson):
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": ljson["user-agent"],
        "Accept-Encoding": "gzip, deflate",
        "user-id": ljson["user-id"],
        "x-bc": ljson["x-bc"],
        "Cookie": "sess=" + ljson["sess"],
        "app-token": APP_TOKEN,
    }


# Every API request must be signed
def create_signed_headers(link, queryParams):
    path = "/api2/v2" + link
    if queryParams:
        query = "&".join("=".join((key, val)) for (key, val) in queryParams.items())
        path = f"{path}?{query}"
    unixtime = str(int(dt.datetime.now().timestamp()))
    msg = "\n".join(
        [dynamic_rules["static_param"], unixtime, path, API_HEADER["user-id"]]
    )
    message = msg.encode("utf-8")
    hash_object = hashlib.sha1(message)
    sha_1_sign = hash_object.hexdigest()
    sha_1_b = sha_1_sign.encode("ascii")
    checksum = (
        sum([sha_1_b[number] for number in dynamic_rules["checksum_indexes"]])
        + dynamic_rules["checksum_constant"]
    )
    API_HEADER["sign"] = dynamic_rules["format"].format(sha_1_sign, abs(checksum))
    API_HEADER["time"] = unixtime
    return


# API request convenience function
# getdata and postdata should both be JSON
def api_request(endpoint, getdata=None, postdata=None, getparams=None):
    if getparams == None:
        getparams = {"order": "publish_date_desc"}
    if getdata is not None:
        for i in getdata:
            getparams[i] = getdata[i]

    if postdata is None:
        if getdata is not None:
            # Fixed the issue with the maximum limit of 10 posts by creating a kind of "pagination"

            create_signed_headers(endpoint, getparams)
            list_base = requests.get(
                URL + API_URL + endpoint, headers=API_HEADER, params=getparams
            ).json()
            posts_num = len(list_base)

            if posts_num >= POST_LIMIT:
                beforePublishTime = list_base[POST_LIMIT - 1]["postedAtPrecise"]
                getparams["beforePublishTime"] = beforePublishTime

                while posts_num == POST_LIMIT:
                    # Extract posts
                    create_signed_headers(endpoint, getparams)
                    list_extend = requests.get(
                        URL + API_URL + endpoint, headers=API_HEADER, params=getparams
                    ).json()
                    posts_num = len(list_extend)
                    # Merge with previous posts
                    list_base.extend(list_extend)

                    if posts_num < POST_LIMIT:
                        break

                    # Re-add again the updated beforePublishTime/postedAtPrecise params
                    beforePublishTime = list_extend[posts_num - 1]["postedAtPrecise"]
                    getparams["beforePublishTime"] = beforePublishTime

            return list_base
        else:
            create_signed_headers(endpoint, getparams)
            print("x")
            return requests.get(
                URL + API_URL + endpoint, headers=API_HEADER, params=getparams
            )
    else:
        create_signed_headers(endpoint, getparams)
        return requests.post(
            URL + API_URL + endpoint,
            headers=API_HEADER,
            params=getparams,
            data=postdata,
        )


# /users/<profile>
# get information about <profile>
# <profile> = "customer" -> info about yourself
def get_user_info(profile):
    info = api_request("/users/" + profile).json()
    if "error" in info:
        print("\nERROR: " + info["error"]["message"])
        # bail, we need info for both profiles to be correct
        raise Exception("error")
    return info


# to get subscribesCount for displaying all subs
# info about yourself
def user_me():
    me = api_request("/users/me").json()
    if "error" in me:
        print("\nERROR: " + me["error"]["message"])
        # bail, we need info for both profiles to be correct
        raise Exception("error")
    return me


# get all subscriptions in json
def get_subs():
    SUB_LIMIT = str(user_me()["subscribesCount"])
    params = {
        "type": "active",
        "sort": "desc",
        "field": "expire_date",
        "limit": SUB_LIMIT,
    }
    return api_request("/subscriptions/subscribes", getparams=params).json()


# download public files like avatar and header
new_files = 0


def select_sub():
    # Get Subscriptions
    SUBS = get_subs()
    sub_dict.update({"0": "*** Download All Models ***"})
    ALL_LIST = []
    for i in range(1, len(SUBS) + 1):
        ALL_LIST.append(i)
    for i in range(0, len(SUBS)):
        sub_dict.update({i + 1: SUBS[i]["username"]})
    if len(sub_dict) == 1:
        print("No models subbed")
        raise Exception("error")
    # Select Model
    if ARG1 == "all":
        return ALL_LIST
    MODELS = "1"
    if MODELS == "0":
        return ALL_LIST
    else:
        return [x.strip() for x in MODELS.split(",")]


def download_public_files():
    public_files = ["avatar", "header"]
    for public_file in public_files:
        source = PROFILE_INFO[public_file]
        if source is None:
            continue
        id = get_id_from_path(source)
        file_type = re.findall("\.\w+", source)[-1]
        path = "/" + public_file + "/" + id + file_type
        if not os.path.isfile("profiles/" + PROFILE + path):
            print("Downloading " + public_file + "...")
            download_file(PROFILE_INFO[public_file], path)
            global new_files
            new_files += 1


# download a media item and save it to the relevant directory
def download_media(media, is_archived, file_name):
    id = str(media["id"])
    source = media["source"]["source"]

    if (
        media["type"] != "photo" and media["type"] != "video" and media["type"] != "gif"
    ) or not media["canView"]:
        return

    # find extension
    ext = re.findall("\.\w+\?", source)
    if len(ext) == 0:
        return
    ext = ext[0][:-1]

    # classify the gif
    if media["type"] == "gif":
        type = "video"
    else:
        type = media["type"]

    if is_archived:
        path = "/archived/"
    else:
        path = "/"
    path += type + "s/" + id + ext

    if not os.path.isfile("profiles/" + PROFILE + path):
        # print(path)
        global new_files
        new_files += 1
        download_file(source, file_name)


# helper to generally download files
def download_file(source, file_name_single):
    r = requests.get(source, stream=True)
    file_name = "profiles/" + file_name_single
    with open(file_name, "wb") as f:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, f)

    bucket_endpoint = os.environ.get("BUCKET_ENDPOINT")
    bucket_access = os.environ.get("BUCKET_ACCESS")
    bucket_secret = os.environ.get("BUCKET_SECRET")
    bucket_name = os.environ.get("BUCKET_NAME")

    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=bucket_access,
        aws_secret_access_key=bucket_secret,
    )

    s3.upload_file(file_name, bucket_name, file_name_single)
    s3.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=file_name_single)
    os.remove("profiles/" + file_name_single)


def get_id_from_path(path):
    last_index = path.rfind("/")
    second_last_index = path.rfind("/", 0, last_index - 1)
    id = path[second_last_index + 1 : last_index]
    return id


def calc_process_time(starttime, arraykey, arraylength):
    timeelapsed = time.time() - starttime
    timeest = (timeelapsed / arraykey) * (arraylength)
    finishtime = starttime + timeest
    finishtime = dt.datetime.fromtimestamp(finishtime).strftime("%H:%M:%S")  # in time
    lefttime = dt.timedelta(
        seconds=(int(timeest - timeelapsed))
    )  # get a nicer looking timestamp this way
    timeelapseddelta = dt.timedelta(seconds=(int(timeelapsed)))  # same here
    return (timeelapseddelta, lefttime, finishtime)


# iterate over posts, downloading all media
# returns the new count of downloaded posts
def download_posts(cur_count, posts, is_archived, row, user_id):
    for k, post in enumerate(posts, start=1):
        if "media" not in post or ("canViewMedia" in post and not post["canViewMedia"]):
            continue

        for media in post["media"]:
            if "source" in media:
                if media["type"] == "photo":
                    rd = random.randint(10000000, 99999999)
                    file_name = f"{user_id}_{rd}.jpg"
                    file_url = f"https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{user_id}_{rd}.jpg"
                    download_media(media, is_archived, file_name)
                    row.append((user_id, "IMAGE", file_url))
                if media["type"] == "video":
                    rd = random.randint(10000000, 99999999)
                    file_name = f"{user_id}_{rd}.mp4"
                    file_url = f"https://vude-bucket.blr1.digitaloceanspaces.com/test-dev/{user_id}_{rd}.mp4"
                    download_media(media, is_archived, file_name)
                    row.append((user_id, "VIDEO", file_url))

        # adding some nice info in here for download stats
        timestats = calc_process_time(starttime, k, total_count)
        dwnld_stats = (
            f"{cur_count}/{total_count} {round(((cur_count / total_count) * 100))}% "
            + "Time elapsed: %s, Estimated Time left: %s, Estimated finish time: %s"
            % timestats
        )
        end = "\n" if cur_count == total_count else "\r"
        print(dwnld_stats, end=end)

        cur_count = cur_count + 1

    return cur_count


def get_all_videos(videos):
    len_vids = len(videos)
    has_more_videos = False
    if len_vids == 50:
        has_more_videos = True

    while has_more_videos:
        has_more_videos = False
        len_vids = len(videos)
        extra_video_posts = api_request(
            "/users/" + PROFILE_ID + "/posts/videos",
            getdata={
                "limit": str(POST_LIMIT),
                "order": "publish_date_desc",
                "beforePublishTime": videos[len_vids - 1]["postedAtPrecise"],
            },
        )
        videos.extend(extra_video_posts)
        if len(extra_video_posts) == 50:
            has_more_videos = True

    return videos


def get_all_photos(images):
    len_imgs = len(images)
    has_more_images = False
    if len_imgs == 50:
        has_more_images = True

    while has_more_images:
        has_more_images = False
        len_imgs = len(images)
        extra_img_posts = api_request(
            "/users/" + PROFILE_ID + "/posts/photos",
            getdata={
                "limit": str(POST_LIMIT),
                "order": "publish_date_desc",
                "beforePublishTime": images[len_imgs - 1]["postedAtPrecise"],
            },
        )
        images.extend(extra_img_posts)
        if len(extra_img_posts) == 50:
            has_more_images = True

    return images


def onlyfans_downloader_script(authData):
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("~ Scrapper Scripts start ~")
    print("~  .....wait....  ~")
    print("~  .....wait....  ~")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

    # Gather inputs
    if len(sys.argv) != 2:
        global ARG1
        ARG1 = ""
    else:
        ARG1 = sys.argv[1]

    # Get the rules for the signed headers dynamically, as they may be fluid
    r = {
        "static_param": "ZQA9HPtVxkqF292DHHQlYNhx7WRQsAKU",
        "format": "13190:{}:{:x}:653286c6",
        "checksum_indexes": [
            11,
            17,
            13,
            20,
            15,
            39,
            35,
            14,
            26,
            14,
            15,
            19,
            36,
            14,
            10,
            28,
            34,
            36,
            24,
            7,
            30,
            0,
            31,
            37,
            35,
            34,
            7,
            1,
            5,
            35,
            30,
            27,
        ],
        "checksum_constants": [
            115,
            100,
            -92,
            138,
            -141,
            100,
            146,
            120,
            103,
            111,
            -107,
            -96,
            103,
            50,
            -136,
            -66,
            -87,
            102,
            -71,
            -107,
            -88,
            71,
            123,
            -109,
            95,
            91,
            -78,
            -112,
            -81,
            76,
            71,
            -72,
        ],
        "checksum_constant": 272,
        "app_token": "33d57ade8c02dbc5a333db99ff9ae26a",
        "remove_headers": ["user-id"],
        "error_code": 0,
        "message": "",
    }
    global dynamic_rules
    dynamic_rules = json.loads(json.dumps(r))
    # Create Header
    global API_HEADER
    API_HEADER = create_auth(authData)

    # Select sub
    global sub_dict
    sub_dict = {}
    SELECTED_MODELS = select_sub()

    # start process
    for M in SELECTED_MODELS:
        global PROFILE
        PROFILE = sub_dict[int(M)]
        global PROFILE_INFO
        PROFILE_INFO = get_user_info(PROFILE)
        global PROFILE_ID
        PROFILE_ID = authData["user-id"]

        print("\n onlyfans-dl is downloading content to profiles/" + PROFILE + "!\n")

        if os.path.isdir("profiles/" + PROFILE):
            print("\nThe folder profiles/" + PROFILE + " exists.")
            print("Media already present will not be re-downloaded.")

        assure_dir("profiles")

        # first save profile info
        print("Saving profile info...")

        # sinf = {
        #     "id": PROFILE_INFO["id"],
        #     "name": PROFILE_INFO["name"],
        #     "username": PROFILE_INFO["username"],
        #     "about": PROFILE_INFO["rawAbout"],
        #     "joinDate": PROFILE_INFO["joinDate"],
        #     "website": PROFILE_INFO["website"],
        #     "wishlist": PROFILE_INFO["wishlist"],
        #     "location": PROFILE_INFO["location"],
        #     "lastSeen": PROFILE_INFO["lastSeen"]
        # }

        # with open("profiles/" + PROFILE + "/info.json", 'w') as infojson:
        #     json.dump(sinf, infojson)

        # download_public_files()

        # get all user posts
        print("Finding photos...", end=" ", flush=True)
        photos = api_request(
            "/users/" + PROFILE_ID + "/posts/photos", getdata={"limit": str(POST_LIMIT)}
        )
        photo_posts = get_all_photos(photos)
        print("Found " + str(len(photo_posts)) + " photos.")
        print("Finding videos...", end=" ", flush=True)
        videos = api_request(
            "/users/" + PROFILE_ID + "/posts/videos", getdata={"limit": str(POST_LIMIT)}
        )
        video_posts = get_all_videos(videos)
        print("Found " + str(len(video_posts)) + " videos.")
        print("Finding archived content...", end=" ", flush=True)
        archived_posts = api_request(
            "/users/" + PROFILE_ID + "/posts/archived",
            getdata={"limit": str(POST_LIMIT)},
        )
        print("Found " + str(len(archived_posts)) + " archived posts.")
        postcount = len(photo_posts) + len(video_posts)
        archived_postcount = len(archived_posts)
        if postcount + archived_postcount == 0:
            print("ERROR: 0 posts found.")
            raise Exception("error")
        global total_count
        total_count = postcount + archived_postcount

        print("Found " + str(total_count) + " posts. Downloading media...")

        # get start time for estimation purposes
        global starttime
        starttime = time.time()

        row = []
        cur_count = download_posts(1, photo_posts, False, row, authData["user-id"])
        cur_count = download_posts(
            cur_count, video_posts, False, row, authData["user-id"]
        )
        df = pd.DataFrame(row, columns=["onlyfans_id", "media_type", "media_url"])
        df["user_id"] = authData["vude-id"]
        df["file_id"] = (
            df["user_id"].astype(str)
            + "_"
            + (df.groupby("onlyfans_id").cumcount() + 1).astype(str)
        )
        connection_string = os.environ.get("DB_CONNECTION_STRING")
        engine = create_engine(connection_string)
        pg.upsert(
            con=engine,
            df=df.set_index("file_id"),
            table_name="import_onlyfans",
            if_row_exists="update",
            schema="public",
            chunksize=10000,
        )
        # download_posts(cur_count, archived_posts, True, row, "ARCHIVE", authData['user-id'])
        print("Downloaded " + str(new_files) + " new files.")
