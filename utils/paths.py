"""Global paths"""
import os

from .strings import CONDA_PREFIX

UTILS_DIRNAME = "utils"
WORD_DIRNAME = "word"
THUMBNAIL_DIRNAME = "thumbnail"
DCCON_DIRNAME = "dccon"
TABLE_DIRNAME = "table"
PROFILEPIC_DIRNAME = "profile_pic"

KONLPY_USERDICT_FILENAME = "lib/python3.8/site-packages/konlpy/java/data/kE/dic_user.txt"
KOTLIN_FILENAME = "KotlinInside-1.14.6-fat.jar"
POST_FILENAME = "post.json"
COMMENT_FILENAME = "comment.json"
WORD_DAY_FILENAME = "_day.json"
WORD_NIGHT_FILENAME = "_night.json"
WORDCLOUD_SRC_FILENAME = "ai.jpg"
WORDCLOUD_SAVE_FILENAME = "wordcloud.png"
USERDICT_FILENAME = "user_dict.txt"
TOTALTAGS_FILENAME = "total_count.csv"
DCCONRANK_FILENAME = "dccon_rank.csv"
VTUBERRANK_FILENAME = "sum_ai.csv"
NIJIRANK_FILENAME = "sum_holo.csv"
HOLORANK_FILENAME = "sum_holo.csv"
NOHOLORANK_FILENAME = "sum_noholo.csv"
YOUTUBE_FILENAME = "youtube.csv"
TEMPLATE_FILENAME = "template.pptx"

HOME = os.path.abspath("./")
KOTLIN_HOME = os.path.abspath("../KotlinInside/build/libs")
JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"
PROFILEPIC_DIR = os.path.join(HOME, UTILS_DIRNAME, PROFILEPIC_DIRNAME)
CONDA_PREFIX = os.getenv(CONDA_PREFIX)
if CONDA_PREFIX is None:
    raise RuntimeError("Please setup with conda and konlpy")
CUSTOMDICT_FILE = f"{CONDA_PREFIX}/{KONLPY_USERDICT_FILENAME}"
WORDCLOUD_FILE = os.path.join(HOME, UTILS_DIRNAME, WORDCLOUD_SRC_FILENAME)
USERDICT_FILE = os.path.join(HOME, UTILS_DIRNAME, USERDICT_FILENAME)
POWERPOINT_FILE = os.path.join(HOME, UTILS_DIRNAME, TEMPLATE_FILENAME)
FONT_FILE = "/mnt/c/Windows/Fonts/BMDOHYEON_ttf.ttf"
