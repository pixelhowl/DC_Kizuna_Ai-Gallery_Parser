# Profile
import os
import re
import requests
from bs4 import BeautifulSoup as soup
import urllib.request
import urllib.parse
import pandas as pd

from . import *
from . import vtuber_dict
from . import parsers

YOUTUBE_PROFILE_IMAGE_START_URL = "yt3.(ggpht|googleusercontent).com/"
YOUTUBE_PROFILE_IMAGE_END_URL = "-c-k-c0x00ffffff-no-rj"
PROFILE_IMAGE_URL = re.compile(YOUTUBE_PROFILE_IMAGE_START_URL + "(.*?)" +
                               YOUTUBE_PROFILE_IMAGE_END_URL)
YOUTUBE_THUMBNAIL_IMAGE_START_URL = "https://i.ytimg.com/vi/"
YOUTUBE_THUMBNAIL_IMAGE_END_URL = "/maxresdefault.jpg"

PROFILE_PIC_DIR = os.path.abspath(f"./utils/profile_pic/")


def donwload_youtube_channel_profile_image(vtuber_name, *, use_cache=True):
    filename = f'{vtuber_name}.png'
    profile_imagefile = os.path.join(PROFILE_PIC_DIR, filename)
    if use_cache and os.path.isfile(profile_imagefile):
        parsers.LOGGER.info(f"{vtuber_name} profile image already exist...")
        return False
    if not vtuber_name in vtuber_dict.youtube_channel_id:
        parsers.LOGGER.error(
            f"{vtuber_name} cannot find vtuber channel id. Please update.")
        return False
    channel_id = vtuber_dict.youtube_channel_id[vtuber_name]
    web_url = f"https://www.youtube.com/{channel_id}"
    with urllib.request.urlopen(web_url) as response:
        html = response.read()
        bs = soup(html, "html.parser")
        link = bs.find_all('link', {
            'href': PROFILE_IMAGE_URL,
            "itemprop": "thumbnailUrl"
        })
    if len(link) == 0:
        print(bs.prettify())
        raise RuntimeError(f"It cannot find link with {vtuber_name}")
    profile_url = link[0]["href"]
    img_data = requests.get(profile_url).content
    with open(profile_imagefile, 'wb') as f:
        f.write(img_data)
        f.close()

    return True


def donwload_youtube_video_thumbnail_image(video_id, *, use_cache=True):
    video_thumbnail_dir = os.path.join(parsers.CUR_DIR, "thumbnail")
    thumbnail_filename = f"{video_id}.png"
    thumbnail_imagefile = os.path.join(video_thumbnail_dir, thumbnail_filename)
    if use_cache and os.path.isfile(thumbnail_imagefile):
        parsers.LOGGER.info(f"{video_id} thumbnail image already exist...")
        return False
        
    thumbnail_url = YOUTUBE_THUMBNAIL_IMAGE_START_URL + video_id + YOUTUBE_THUMBNAIL_IMAGE_END_URL
    img_data = requests.get(thumbnail_url).content
    with open(thumbnail_imagefile, 'wb') as f:
        f.write(img_data)
        f.close()
    
    return True


class youtubeDownloader():

    def __init__(self, use_cache=True):
        self.use_cache = use_cache

    def download_top_k(self, filename, *, k=10):
        if filename.find('youtube') != -1:
            download_func = donwload_youtube_video_thumbnail_image
            col_name = 'VideoID'
        else:
            download_func = donwload_youtube_channel_profile_image
            col_name = 'Vtuber'
            
        tags_filename = os.path.join(parsers.CUR_DIR, filename)
        if os.path.isfile(tags_filename):
            parsers.LOGGER.info("Using cached tags...")
            df = pd.read_csv(tags_filename)
        else:
            # parsers.LOGGER.info("Generate with cotent_counter...")
            raise RuntimeError(f"It must have {tags_filename} file...")

        for idx, row in df.head(n=k).iterrows():
            data = row[col_name]
            download_func(data, use_cache=self.use_cache)

    def run(self):
        self.download_top_k('sum_ai.csv')
        self.download_top_k('sum_noholo.csv')
        self.download_top_k('youtube.csv')