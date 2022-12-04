import base64
import calendar
import collections
import json
import logging
import os
import re
import shutil
import PIL

import gevent.monkey
import gevent.pool

gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
import requests

# TODO(pixelhowl): Use kiwi after complex noun supports
# import kiwipiepy
from tqdm.notebook import tqdm
import konlpy.tag
import numpy as np
import pandas as pd
import wordcloud
import ray
import urllib.parse as urlparse

from . import *
from . import vtuber_dict
from . import database

# 로깅
LOGGER = logging.getLogger("kizunaai")
LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter(
    '[%(asctime)s][%(name)s][%(levelname)s] %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
LOGGER.addHandler(stream_handler)

# 웹 크롤링을 위한 전역 변수
DELETE_URL = r"\uae00\uc5c6\uc74c"
SESSION = requests.Session()
SESSION_TIMEOUT = 5
SESSION_RETRIAL_COUNT = 5
TRIAL_WITH_APP_ID = 300

CUR_DIR = os.path.abspath(f"./{GALLARY_NAME}/{CUR_YEAR}/{CUR_MONTH}/")
PREV_YEAR = CUR_YEAR
PREV_MONTH = CUR_MONTH - 1
if CUR_MONTH == 1:
    PREV_MONTH = 12
    PREV_YEAR = CUR_YEAR - 1
PREV_DIR = os.path.abspath(f"./{GALLARY_NAME}/{PREV_YEAR}/{PREV_MONTH}/")

POST_FILE = os.path.join(CUR_DIR, POST_FILENAME)
COMMENT_FILE = os.path.join(CUR_DIR, COMMENT_FILENAME)
VIDEO_THUMBNAIL_DIR = os.path.join(CUR_DIR, "thumbnail")

# 단어 추출 전역 변수
USER_DICT_FILE = os.path.join(HOME, 'utils', "user_dict.txt")
CONDA_PREFIX = os.getenv("CONDA_PREFIX")
if CONDA_PREFIX == None:
    raise RuntimeError("Please setup with conda and konlpy")
CUSTOM_DICTFILE = f"{CONDA_PREFIX}/lib/python3.8/site-packages/konlpy/java/data/kE/dic_user.txt"

SETENCE_FILTER = []
WORD_FILTER = [
    "!!!", ".com", ".kr", ".co", ".be", "!!", "A1", "A2", "E8", "E3", "DU",
    "QE", "175", "M44", "WY", "-2", "E3", ".jp", "M4", "-5", "-4", "A9", "00",
    "B1", "E7", "A4", "B4", "-1", "B7", "RY", "B6", "P2", "A3", "D6", "DI",
    "A1", "M1", "5D", "-D", "C3", "-0", "B2", "T8", "A7", "H1", "X5", "S3",
    "PE", "I3", "S6", "E4", "-3", ".tv", ".net"
]
DCCON_FILTER = []
DCCON_REGEX = r'(https?://)?(www\.)?dcimg[0-9]\.dcinside\.com\/dccon\.php\?no=[a-z0-9]+'
YOUTUBE_REGEX = r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})'

TAGS_ID = 'NNP'

# 랭킹 전역 변수
TOP_NUM = 10
BAR_MAX_NUM = 18.56


class contentCounter():

    def __init__(self, tags_df_file):
        self.userdict_df = pd.read_csv(USER_DICT_FILE,
                                       names=["단어", "품사"],
                                       index_col=None,
                                       sep='\t')
        self.copy_user_dict_flag = False
        self.analyzer = self.get_analyzer()
        self.one_word_filter = self.userdict_df["단어"][
            self.userdict_df["단어"].str.len() == 1].tolist()
        self.tags = collections.defaultdict(int)
        self.vtuber_post_comment_counter = collections.defaultdict(int)
        self.tags_df_file = tags_df_file

    def copy_custom_dict(self):
        if self.copy_user_dict_flag:
            shutil.copy(USER_DICT_FILE, CUSTOM_DICTFILE)

    def add_custom_word_to_dict(self):
        # 자동 사전 단어 추가 vtuber_dict.py에만 추가하면 됨
        for _, nicknames in tqdm(vtuber_dict.vtuber.items(),
                                 desc="Updating custom dictionary"):
            for nickname in nicknames:
                nickname_list = [nickname, TAGS_ID]
                if not self.userdict_df["단어"].isin(nickname_list).any():
                    self.userdict_df.loc[len(self.userdict_df)] = nickname_list
        self.userdict_df.to_csv(USER_DICT_FILE,
                                header=False,
                                index=False,
                                sep='\t')

    def set_custom_dict(self):
        self.add_custom_word_to_dict()
        self.copy_custom_dict()

    def get_analyzer(self):
        self.set_custom_dict()
        analyzer = konlpy.tag.Komoran(userdic=USER_DICT_FILE)
        return analyzer

    def setence_parser(self, sentence):
        no_youtube_sentence = re.sub(YOUTUBE_REGEX, "", sentence)
        no_encoded_LF_setence = no_youtube_sentence.replace('\\n', '\n')
        setence_array = []
        for str_piece in no_encoded_LF_setence.split("\n"):
            no_blanks_str_piece = str_piece.strip()
            if no_blanks_str_piece:
                setence_array.append(no_blanks_str_piece)

        clean_sentence = "\n".join(setence_array).strip()

        return clean_sentence

    def dict_extract_nouns(self, sentence):
        parsed_sentence = self.setence_parser(sentence)
        try:
            nouns = self.analyzer.nouns(parsed_sentence)
        except Exception as e:
            LOGGER.info("error: %s, content: %s" % (e, parsed_sentence))
        return nouns
        # tokens = ANALYER.tokenize(parsed_sentence)
        # nouns = [t for t in tokens if t.tag.startswith('NN')]
        # return nouns

    def tagging(self, content):
        if len(SETENCE_FILTER) and any(word in content
                                       for word in SETENCE_FILTER):
            return

        nouns = self.dict_extract_nouns(content)
        nouns_list = list(set(nouns))
        count = collections.Counter(nouns_list)

        count_list = list(count)

        for word in count_list:
            if not word in self.one_word_filter and len(word) == 1:
                del count[word]

        for word in list(count.elements()):
            self.tags[word] += 1

            if word in vtuber_dict.nicknames:
                for vtuber, calls in vtuber_dict.vtuber.items():
                    if word in calls:
                        self.vtuber_post_comment_counter[vtuber] += 1
                        break

    def tagging_with_timeline(self, df, time_cond, word_file_name, timeline):
        time_cond_df = df[time_cond]
        lines = time_cond_df[u'제목내용']
        lines = lines.dropna().tolist()

        for line in lines:
            line_str = str(line)
            self.tagging(line_str)

        word_file = os.path.join(CUR_DIR, WORD_DIR,
                                 f"{str(timeline.day)}{word_file_name}")
        # FIXME(pixelhowl): async writing
        with open(word_file, 'w') as f:
            json.dump(self.tags.copy(), f, ensure_ascii=False)

        self.tags = collections.defaultdict(int)

    def tagging_with_timelines(self, df, time_conds, word_file_names,
                               timeline):
        for time_cond, word_file_name in zip(time_conds, word_file_names):
            self.tagging_with_timeline(df, time_cond, word_file_name, timeline)

    def get_total_word_dict(self):
        words = []
        last_day = calendar.monthrange(CUR_YEAR, CUR_MONTH)[1]

        for beg in range(1, last_day + 1):
            day_file_path = os.path.join(CUR_DIR, WORD_DIR,
                                         f"{str(beg)}{WORD_DAY_FILENAME}")
            night_file_path = os.path.join(CUR_DIR, WORD_DIR,
                                           f"{str(beg)}{WORD_NIGHT_FILENAME}")

            with open(day_file_path, "r") as f:
                json_file = dict(json.load(f))
                words.append(json_file)

            with open(night_file_path, "r") as f:
                json_file = dict(json.load(f))
                words.append(json_file)

        counter = collections.Counter()

        for tag in words:
            counter.update(tag)

        words_dict = dict(counter)
        return words_dict

    def run(self):
        content_df = database.get_content_df()
        content_df[u'날짜'] = pd.to_datetime(content_df[u'날짜'],
                                           format='%Y.%m.%d')

        first_timeline = f'{CUR_YEAR}-{CUR_MONTH}'
        last_timeline = f'{CUR_YEAR}-{CUR_MONTH+1}' if CUR_MONTH != 12 else f'{CUR_YEAR+1}-1'

        for cur_timeline in tqdm(pd.date_range(first_timeline,
                                               last_timeline,
                                               inclusive='left',
                                               freq='D'),
                                 desc="Word counting"):
            day_time = cur_timeline
            afternoon_time = cur_timeline + datetime.timedelta(hours=12)
            night_time = cur_timeline + datetime.timedelta(hours=24)

            day_to_night_cond = (content_df[u'날짜'] >=
                                 day_time) & (content_df[u'날짜'] < night_time)

            time_cond_df = content_df[day_to_night_cond]

            day_to_afternoon_cond = (time_cond_df[u'날짜'] >= day_time) & (
                time_cond_df[u'날짜'] < afternoon_time)
            afternoon_to_night_cond = (time_cond_df[u'날짜'] >= afternoon_time
                                       ) & (time_cond_df[u'날짜'] < night_time)
            time_conds = [day_to_afternoon_cond, afternoon_to_night_cond]
            file_names = [WORD_DAY_FILENAME, WORD_NIGHT_FILENAME]
            self.tagging_with_timelines(time_cond_df, time_conds, file_names,
                                        cur_timeline)

        vtbuer_post_comment_file = os.path.join(CUR_DIR,
                                                "total_post_count.csv")
        vtbuer_post_comment_df = pd.DataFrame(
            self.vtuber_post_comment_counter.items(),
            columns=["Vtuber", "글/댓글 수"])
        vtbuer_post_comment_df.sort_values(by=['글/댓글 수'],
                                           ascending=False,
                                           inplace=True)
        result = vtbuer_post_comment_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(vtbuer_post_comment_file)

        tags_dict = self.get_total_word_dict()
        tags_df = pd.DataFrame(tags_dict.items(), columns=["단어", "언급 수"])
        tags_df = tags_df[~tags_df["단어"].isin(WORD_FILTER)]
        tags_df.sort_values(by=['언급 수'], ascending=False, inplace=True)
        result = tags_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(self.tags_df_file)
        return tags_dict


class youtubeCounter():

    def __init__(self):
        self.columns = ["ChannelName", "VideoID", "Count"]
        ray.init()

    def yotube_rank(self):
        content_df = database.get_content_df()
        youtube_df = content_df[content_df['제목내용'].str.contains(
            YOUTUBE_REGEX, na=False, regex=True)][['제목내용']]

        yt_regex_id = ray.put(YOUTUBE_REGEX)
        youtube_rank_file = os.path.join(CUR_DIR, 'youtube.csv')

        if os.path.isfile(youtube_rank_file):
            yt_df = pd.read_csv(youtube_rank_file, index_col=None)
            yt_df["VideoID"] = yt_df["VideoID"].str.replace(
                r"[가-힣|ㄱ-ㅎ|ㅏ-ㅣ ]+", "", regex=True
            ).str.replace(
                r"(https://www.youtube.com/watch)|(https://m.youtube.com/watch)",
                "",
                regex=True)
            yt_df["Count"] = 0
        else:
            yt_df = pd.DataFrame(columns=self.columns)

        def video_id_parse(value):
            """
            Examples:
            - http://youtu.be/SA2iWivDJiE
            - http://www.youtube.com/watch?v=_oPAwA_Udwc&feature=feedu
            - http://www.youtube.com/embed/SA2iWivDJiE
            - http://www.youtube.com/v/SA2iWivDJiE?version=3&amp;hl=en_US
            """

            query = urlparse.urlparse(value)
            if query.hostname == 'youtu.be':
                return query.path[1:]
            if query.hostname in ('www.youtube.com', 'youtube.com'):
                if query.path == '/watch?app=desktop':
                    p = query.path.split()
                if query.path == '/watch':
                    p = urlparse.parse_qs(query.query)
                    if not 'v' in p:
                        print(value)
                        print(p)
                    return p['v'][0]
                if query.path[:7] == '/embed/':
                    return query.path.split('/')[2]
                if query.path[:3] == '/v/':
                    return query.path.split('/')[2]
            # fail?
            return None

        @ray.remote
        def parse_contents(contents, yt_regex):
            processed_video_ids = []
            for content in contents:
                processed_content = str(content).replace("http", " http")
                video_ids = []
                for x in re.finditer(yt_regex, processed_content):

                    video_ids.extend(x.group().replace("amp;", "").split(" "))

                for raw_video_id in video_ids:
                    video_id = str(video_id_parse(str(raw_video_id)))
                    if video_id is None:
                        continue
                    processed_video_ids.append([video_id])

            return processed_video_ids

        result_ids = []
        contents = []
        batch_size = 1000
        for _, row in tqdm(youtube_df.iterrows(),
                           total=len(youtube_df),
                           desc="Planning parsing"):
            content = row["제목내용"]
            contents.append(content)
            if len(contents) >= batch_size:
                result_ids.append(parse_contents.remote(contents, yt_regex_id))
                contents = []

        with tqdm(total=len(result_ids),
                  desc="Parsing youtube link") as pbar:  # 소요 시간 출력
            yt_arr = []
            while len(result_ids):
                done_id, result_ids = ray.wait(result_ids)
                test = ray.get(done_id)

                video_ids = test[0]
                yt_arr.extend(video_ids)
                pbar.update(1)

        raw_yt_df = pd.DataFrame(yt_arr, columns=["VideoID"])
        raw_yt_df = raw_yt_df[raw_yt_df["VideoID"] != "None"]
        yt_df = raw_yt_df.value_counts().to_frame(name="Count")

        yt_df.sort_values(by=['Count'], ascending=False, inplace=True)
        yt_df.to_csv(youtube_rank_file, sep=',')

    def run(self):
        self.yotube_rank()
        LOGGER.info("yotube_rank finished.")


class wordRanker():

    def __init__(self, use_cache=True):
        self.savefolder = CUR_DIR
        self.encoder = "utf8"
        self.tags_df_file = os.path.join(CUR_DIR, "total_count.csv")
        if use_cache and os.path.isfile(self.tags_df_file):
            LOGGER.info("Using cached tags...")
            tags_df = pd.read_csv(self.tags_df_file, index_col=0)
            self.word_dict = tags_df.set_index("단어").to_dict()['언급 수']
        else:
            LOGGER.info("Generate with cotent_counter...")
            self.content_counter = contentCounter(self.tags_df_file)
            self.word_dict = self.content_counter.run()

    def word_cloud(self):
        for word in WORD_FILTER:
            self.word_dict.pop(word, None)

        maskfile = np.array(
            PIL.Image.open(os.path.join(HOME, "utils", "ai.jpg")))
        wc = wordcloud.WordCloud(
            font_path=r'/mnt/c/Windows/Fonts/BMDOHYEON_ttf.ttf',
            mode="RGBA",
            background_color=None,
            max_words=1000,
            mask=maskfile,
            prefer_horizontal=1,
            min_font_size=0,
            max_font_size=150,
            width=1398.425197,
            height=2116.535433,
            scale=5).generate_from_frequencies(self.word_dict)
        image_colors = wordcloud.ImageColorGenerator(maskfile)
        wc.recolor(color_func=image_colors,
                   random_state=0).to_file(f"{CUR_DIR}/wordcloud.png")
        LOGGER.info("Word cloud generated.")

    def dccon_downloader(self, rank, dccon_url):
        response = SESSION.get(dccon_url)
        dccon_filename = f'{rank}.gif'
        dccon_file = os.path.join(CUR_DIR, "dccon", dccon_filename)
        with open(dccon_file, 'wb') as f:
            f.write(response.content)
            f.close()
        response.close()

    def dccon_rank(self):
        comment_df = pd.read_json(COMMENT_FILE)
        comment_df = comment_df[comment_df['dccon'] == True]
        dccons_counter = comment_df['content'].value_counts()

        dccon_count_file = os.path.join(CUR_DIR, "dccon_rank.csv")
        dccon_count_df = pd.DataFrame(dccons_counter.items(),
                                      columns=["디시콘", "사용 수"])
        dccon_count_df.sort_values(by=['사용 수'], ascending=False, inplace=True)
        result = dccon_count_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(dccon_count_file)
        LOGGER.info("dccon_rank finished.")

    def dccon_download(self):
        dccon_count_file = os.path.join(CUR_DIR, "dccon_rank.csv")
        dccon_df = pd.read_csv(dccon_count_file, index_col=0)

        for idx, row in dccon_df.head(n=10).iterrows():
            rank = idx
            dccon_url = row['디시콘']
            self.dccon_downloader(rank, dccon_url)

        LOGGER.info("dccon_download finished.")

    def word_rank(self, vtuber_name_dict, output_filename):
        log = []
        if not os.path.exists(self.savefolder):
            os.makedirs(self.savefolder)

        writer_path = os.path.join(self.savefolder, output_filename)

        for vtuber in vtuber_name_dict:
            total_call = 0
            tags = {}
            for nickname in vtuber_name_dict[vtuber]:
                if nickname in self.word_dict.keys():
                    count = self.word_dict[nickname]
                    total_call += count
                    tags[nickname] = count
                else:
                    tags[nickname] = 0

            sorted_dict = sorted(tags.items(),
                                 key=(lambda x: x[1]),
                                 reverse=True)
            summary = ""
            for nickname, values in sorted_dict:
                if values != 0:
                    summary += f"{nickname}({values})+"
            summary = summary[:-1]

            log.append([vtuber, summary, total_call])

        new_data = pd.DataFrame(log, columns=["Vtuber", "단어(언급 수)", "총 언급 횟수"])
        new_data.sort_values(by=["총 언급 횟수"], ascending=False, inplace=True)

        new_data.index = np.arange(1, len(new_data) + 1)
        try:
            old_data = pd.read_csv(os.path.join(PREV_DIR, output_filename))
        except:
            old_data = None

        standard = new_data.iat[0, 2]
        barlength = BAR_MAX_NUM / standard
        LOGGER.debug(f"new_data: {new_data}")
        for i in range(1, TOP_NUM):
            LOGGER.debug(f"{i+1}번째 바 길이: {new_data.iat[i,2]*barlength}")
        if old_data is not None:
            out_vtuber_df = old_data.head(n=TOP_NUM)[~old_data["Vtuber"].head(
                n=TOP_NUM).isin(new_data["Vtuber"].head(n=TOP_NUM))]
            out_vtuber_list = new_data[new_data["Vtuber"].isin(
                out_vtuber_df["Vtuber"])]
            LOGGER.info(f"Top{TOP_NUM}에서 사라진: {out_vtuber_list}")

        compared_ranks = []
        for idx, member in new_data.iterrows():
            vtuber = member["Vtuber"]
            if old_data is not None and any(old_data["Vtuber"] == vtuber):
                old_row = old_data[old_data["Vtuber"] == vtuber]
                old_rank = int(old_row['Unnamed: 0'])
            else:
                old_rank = "NA"

            if old_rank == "NA":
                compared_rank = f"NEW"
            elif idx > old_rank:
                compared_rank = f"▼{idx-old_rank}"
            elif idx < old_rank:
                compared_rank = f"▲{old_rank-idx}"
            elif idx == old_rank:
                compared_rank = f"■-"
            else:
                compared_rank = "Error"
            compared_ranks.append(compared_rank)

        new_data["전월 대비 순위"] = compared_ranks
        result = new_data.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(writer_path, sep=",", index=True)

    def run(self):
        self.word_rank(vtuber_dict.nijisanji, 'sum_niji.csv')
        self.word_rank(vtuber_dict.hololive, 'sum_holo.csv')
        self.word_rank(vtuber_dict.vtuber, 'sum_ai.csv')
        self.word_rank(vtuber_dict.exceptholo, 'sum_noholo.csv')
        LOGGER.info("word_rank finished.")


def interpret_url(url):
    return base64.b64encode(url.encode("utf-8")).decode("utf-8")


def get_app_id(auth):
    """공앱에서 사용하는 app_id 값 받아오기. 주기적으로 새로 발급해야함.

    Args:
        auth (KotlinInside.security.Auth): jpype Auth class. 코틀린인사이드에서 받아와야함

    Returns:
        str: dcinisde 모바일 app id 값
    """
    app_id = kotilnside.generate_app_id(auth)
    LOGGER.info("Get app_id=%s" % app_id)
    return app_id


def get_html(page_num, app_id, post_list, comment_list):  #글 수집
    post_encode_url = f"{DCINSIDE_URL}/gall_view_new.php?id={GALLARY_NAME}&no={page_num}&app_id={app_id}"
    page_url = f"{DCINSIDE_URL}/redirect.php?hash={interpret_url(post_encode_url)}"
    headers = {"User-Agent": "dcinside.app"}

    for trial_count in range(SESSION_RETRIAL_COUNT):
        try:
            post_page = SESSION.get(page_url,
                                    headers=headers,
                                    timeout=SESSION_TIMEOUT)
            if DELETE_URL in post_page.text or "글없음" in post_page.text:
                post_page.close()
                return
            intro = post_page.json(strict=False)[0]["view_info"]
            view = post_page.json(strict=False)[0]["view_main"]
            #글 내용
            post_content = view["memo"]  
            #작성자 IP
            post_ip = intro["user_id"] if len(
                intro["ip"]) == 0 else intro["ip"]
            #모바일 체크
            mobile = not intro["write_type"] == "W"
            #개념글 체크
            recom = not intro["recommend_chk"] == "N"  
            post_page.close()
            break
        except (requests.exceptions.RequestException, requests.Timeout) as e:
            pass
        except Exception as e:
            if "NotFound" in post_page.text and "404" in post_page.text:
                LOGGER.info(
                    f"Post Trial: {trial_count}, break with 404 NotFound Error, content: {post_page.text}"
                )
                break
            if not ("<!DOCTYPE html>" in post_page.text
                    or post_page.text == ""):
                LOGGER.info(
                    f"Post Trial: {trial_count}, post_number: {page_num} , error: {str(e)}, content: {post_page.text}"
                )
            post_page.close()
    else:
        return  # 모든 시도 실패

    #댓글
    reply_page = 1
    total_comment = 0
    total_page = 1

    # TODO(pixelhowl): Need to refactor this
    while reply_page <= total_page:
        comment_encode_url = f"{DCINSIDE_URL}/comment_new.php?id={GALLARY_NAME}&no={page_num}&re_page={reply_page}&app_id={app_id}"
        mobile_url = f"{DCINSIDE_URL}/redirect.php?hash={interpret_url(comment_encode_url)}%3D%3D"
        pass_nickname = "deleted"
        pass_ip = "deleted"
        for trial_count in range(SESSION_RETRIAL_COUNT):
            try:
                comment_page = SESSION.get(mobile_url,
                                           headers=headers,
                                           timeout=5)
                comment = comment_page.json(strict=False)[0]
                total_page = int(comment["total_page"])
                for comm in comment["comment_list"]:
                    has_ipData = not "ipData" in comm
                    if "under_step" in comm:
                        target = f"{pass_nickname} ({pass_ip})"
                    else:
                        pass_nickname = comm["name"]
                        pass_ip = comm["user_id"] if has_ipData else comm["ipData"]
                        target = None
                    IP = comm["user_id"] if has_ipData and len(comm["ipData"]) else comm["ipData"]
                    has_dccon = "dccon" in comm
                    removed_by_writer = "is_delete_flag" in comm and "작성자" in comm[
                            "is_delete_flag"]
                    content = comm["comment_memo"] if not has_dccon else comm[
                        "dccon"]

                    comment_list.append({
                        u"번호": page_num,
                        u"날짜": comm["date_time"],
                        u"닉네임": comm["name"],
                        "ID/IP": IP,
                        "idtype": comm["member_icon"],
                        "content": content,
                        "dccon": has_dccon,
                        "답글 대상": target,
                        "댓삭 당한 횟수": removed_by_writer
                    })
                comment_page.close()
                break
            except (requests.exceptions.RequestException,
                    requests.Timeout) as e:
                comment = {"comment_list": []}
            except Exception as e:
                if "\"message\":\"Not Found\"" in comment_page.text and "\"status\":404" in comment_page.text:
                    LOGGER.info(
                        f"Comment Trial: {trial_count}, post_number: {page_num}, break with 404 NotFound Error, content: {comment_page.text}"
                    )
                    comment = {"comment_list": []}
                if not ("<!DOCTYPE html>" in comment_page.text
                        or comment_page.text == ""):
                    LOGGER.info(
                        f"Comment Trial: {trial_count}, post_number: {page_num} , error: {str(e)}, content: {comment_page.text}"
                    )
                comment_page.close()

        total_comment += len(comment["comment_list"])
        reply_page += 1

    post_list.append({
        u"번호": page_num,
        u"제목": intro["subject"],
        u"날짜": intro["date_time"],
        u"닉네임": intro["name"],
        "ID/IP": post_ip,
        "idtype": intro["member_icon"],
        u"조회 수": intro["hit"],
        u"달린 댓글 수": total_comment,
        u"추천 수": view["recommend"],
        u"비추 수": view["nonrecommend"],
        "content": post_content,
        "mobile": mobile,
        u"개념글 수": recom
    })


def get_prev_month_last_page():
    prev_post_file = os.path.join(PREV_DIR, POST_FILENAME)
    post_df = pd.read_json(prev_post_file)
    if len(post_df):
        last_page = post_df[u'번호'].max()
    else:
        raise ValueError(
            "Cannot find last page with nothing in {prev_post_file}")
    return last_page


def run_web_crawler(start_page_num, end_page_num):
    if start_page_num == None:
        start_page_num = get_prev_month_last_page()

    LOGGER.info("Get Auth class from KotlinInside.")
    auth = kotilnside.get_auth()
    LOGGER.info("Create gevent pool.")
    pool = gevent.pool.Pool()

    LOGGER.info("Read post dataframe from %s" % POST_FILE)
    post_df = pd.read_json(POST_FILE)
    LOGGER.info("Read comment dataframe from %s" % POST_FILE)
    comment_df = pd.read_json(COMMENT_FILE)
    app_id = get_app_id(auth)

    for cur_page_num in tqdm(range(start_page_num, end_page_num,
                                   TRIAL_WITH_APP_ID),
                             desc=f"{GALLARY_NAME} Webcrawling..."):
        post_list = []
        comment_list = []

        next_page_num = cur_page_num + TRIAL_WITH_APP_ID

        if next_page_num > end_page_num:
            next_page_num = end_page_num + 1

        for page_num in range(cur_page_num, next_page_num):
            if page_num in post_df["번호"].values:
                continue
            pool.spawn(get_html, page_num, app_id, post_list, comment_list)

        pool.join()

        LOGGER.info("Process %s posts", len(post_list))  # 수집 글 갯수

        if len(post_list) > 0:
            pd.concat([post_df, pd.DataFrame(post_list)],
                      sort=False).reset_index(drop=True).to_json(
                          POST_FILE, force_ascii=False)
            post_df = pd.read_json(POST_FILE)

            app_id = get_app_id(auth)

        if len(comment_list) > 0:
            pd.concat([comment_df, pd.DataFrame(comment_list)],
                      sort=False).reset_index(drop=True).to_json(
                          COMMENT_FILE, force_ascii=False)
            comment_df = pd.read_json(COMMENT_FILE)

    LOGGER.info("Do post processing to dataframe")
    database.post_processing_df()
