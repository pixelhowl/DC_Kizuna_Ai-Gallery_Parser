"""utils to parse DCINSIDE package"""
import datetime
import os

HOME = os.path.abspath("./")
KOTLIN_HOME = os.path.abspath("../KotlinInside/build/libs")
GALLARY_NAME = "kizunaai"  #갤러리 id
CUR_YEAR = START_YEAR = datetime.datetime.now().year
END_YEAR = datetime.datetime.now().year
CUR_MONTH = datetime.datetime.now().month - 1  # 갤러리, 연, 월
POST_FILENAME = "post.json"
COMMENT_FILENAME = "comment.json"
THUMBNAIL_DIR = "thumbnail"
WORD_DIR = "word"
WORD_DAY_FILENAME = "_day.json"
WORD_NIGHT_FILENAME = "_night.json"
DCINSIDE_URL = "http://m.dcinside.com/api"

from . import (database, kotilnside, parsers, pptx_utils, vtuber_dict,
               word_dict, youtube)
