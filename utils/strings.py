"""Global strings"""
import re

GALLARY_NAME = "kizunaai"  #갤러리 id
KOTLININSIDE_PACKAGE_NAME = "be.zvz.kotlininside"
KOTLININSIDE_USERNAME = "ㅇㅇ"
KOTLININSIDE_PASSWORD = "zhxmfflsakstp"
JAVA_PATH_ARGS = "-Djava.class.path={CLASS_PATH}"
CONDA_PREFIX = "CONDA_PREFIX"

TAGS_ID = "NNP"
ENCODER = "UTF-8"

CONTENT_ROWNAME = "content"
DATE_ROWNAME = "날짜"

DCCON_REGEX = r"(https?://)?(www\.)?dcimg[0-9]\.dcinside\.com\/dccon\.php\?no=[a-z0-9]+"
YOUTUBE_REGEX = r"(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})"

DELETE_URL = r"\uae00\uc5c6\uc74c"
DCINSIDE_URL = "http://m.dcinside.com/api"
DCINSIDE_PAGE_PHP = "gall_view_new.php"
DCINSIDE_COMMENT_PHP = "comment_new.php"
DCINSIDE_REDIRECT_PHP = "redirect.php"

DCINSIDE_PAGE_QUERY = f"id={GALLARY_NAME}" + "&no={page_num}&app_id={app_id}"
DCINSIDE_PAGE_URL = f"{DCINSIDE_URL}/{DCINSIDE_PAGE_PHP}?{DCINSIDE_PAGE_QUERY}"
DCINSIDE_COMMENT_QUERY = DCINSIDE_PAGE_QUERY + "&re_page={reply_page}"
DCINSIDE_COMMENT_URL = f"{DCINSIDE_URL}/{DCINSIDE_COMMENT_PHP}?{DCINSIDE_COMMENT_QUERY}"

DCINSIDE_MOBILE_URL = f"{DCINSIDE_URL}/{DCINSIDE_REDIRECT_PHP}" + "?hash={hash}"
DCINISDE_MOBILE_COMMENT_URL = DCINSIDE_MOBILE_URL + "%3D%3D"

YOUTUBE_URL = "https://www.youtube.com/{channel_id}"

YOUTUBE_THUMBNAIL_IMAGE_START_URL = "https://i.ytimg.com/vi/"
YOUTUBE_THUMBNAIL_IMAGE_END_URL = "/maxresdefault.jpg"

YOUTUBE_PROFILE_IMAGE_START_URL = "yt3.(ggpht|googleusercontent).com/"
YOUTUBE_PROFILE_IMAGE_END_URL = "-c-k-c0x00ffffff-no-rj"
PROFILE_IMAGE_URL = re.compile(YOUTUBE_PROFILE_IMAGE_START_URL + "(.*?)" +
                               YOUTUBE_PROFILE_IMAGE_END_URL)

MONTH_PATTERN = r"[0-9]+월"
COUNT_PATTERN = "("
VIDEO_COUNT_PATTERN = r"[0-9]+회"
INCREASE_PATTERN = r"▼|▲|■|NEW"

# Not string but with dictionary
DCINSIDE_HEADER = {"User-Agent": "dcinside.app"}
IMAGE_HTML = {"href": PROFILE_IMAGE_URL, "itemprop": "thumbnailUrl"}
YOUTUBE_URL_TUPLE = ("www.youtube.com", "youtube.com")
