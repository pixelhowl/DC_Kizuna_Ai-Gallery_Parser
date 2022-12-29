"""Parsers functions"""
import base64
import calendar
import collections
import json
import os
import re
import shutil
import time

import gevent.monkey
import gevent.pool
import PIL

gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
import urllib.parse as urlparse
from datetime import datetime

import konlpy.tag
import numpy as np
import pandas as pd
import ray
import requests
import wordcloud
# TODO(pixelhowl): Use kiwi after complex noun supports
# import kiwipiepy
from tqdm.notebook import tqdm

from . import (database, kotilnside, logging, paths, strings, vtuber_dict,
               word_dict)

# pylint: disable=protected-access, unused-variable, use-implicit-booleaness-not-len
# pylint: disable=logging-fstring-interpolation, singleton-comparison
# pylint: disable=consider-using-f-string, logging-not-lazy, broad-except
# pylint: disable=bare-except

# 웹 크롤링을 위한 전역 변수
SESSION = requests.Session()
SESSION_TIMEOUT = 5
SESSION_RETRIAL_COUNT = 5
TRIAL_WITH_APP_ID = 800

# 랭킹 전역 변수
TOP_NUM = 10
BAR_MAX_NUM = 18.56


class ContentCounter():

    def __init__(self, tags_df_file, *, year=None, month=None):
        self.year, self.month = database.get_year_month(year, month)
        self.data_dir = database.get_data_dir(self.year, self.month)
        self.userdict_df = pd.read_csv(paths.USERDICT_FILE,
                                       names=["단어", "품사"],
                                       index_col=None,
                                       sep="\t")
        self.copy_user_dict_flag = False
        self.analyzer = self.get_analyzer()
        self.one_word_filter = self.userdict_df["단어"][
            self.userdict_df["단어"].str.len() == 1].tolist()
        self.tags = collections.defaultdict(int)
        self.vtuber_post_comment_counter = collections.defaultdict(int)
        self.tags_df_file = tags_df_file

    def copy_custom_dict(self):
        if self.copy_user_dict_flag:
            shutil.copy(paths.USERDICT_FILE, paths.CUSTOMDICT_FILE)

    def add_custom_word_to_dict(self):
        # 자동 사전 단어 추가 vtuber_dict.py에만 추가하면 됨
        for _, nicknames in tqdm(vtuber_dict.vtuber.items(),
                                 desc="Updating custom dictionary"):
            for nickname in nicknames:
                nickname_list = [nickname, strings.TAGS_ID]
                if not self.userdict_df["단어"].isin(nickname_list).any():
                    self.userdict_df.loc[len(self.userdict_df)] = nickname_list
        self.userdict_df.to_csv(paths.USERDICT_FILE,
                                header=False,
                                index=False,
                                sep="\t")

    def set_custom_dict(self):
        self.add_custom_word_to_dict()
        self.copy_custom_dict()

    def get_analyzer(self):
        self.set_custom_dict()
        analyzer = konlpy.tag.Komoran(userdic=paths.USERDICT_FILE)
        return analyzer

    def setence_parser(self, sentence):
        no_youtube_sentence = re.sub(strings.YOUTUBE_REGEX, "", sentence)
        no_encoded_lf_setence = no_youtube_sentence.replace("\\n", "\n")
        setence_array = []
        for str_piece in no_encoded_lf_setence.split("\n"):
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
            logging.LOGGER.info(f"error: {e}, content: {parsed_sentence}")
        return nouns
        # tokens = ANALYER.tokenize(parsed_sentence)
        # nouns = [t for t in tokens if t.tag.startswith("NN")]
        # return nouns

    def tagging(self, content):
        if len(word_dict.SETENCE_FILTER) != 0 and any(
                word in content for word in word_dict.SETENCE_FILTER):
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
        lines = time_cond_df["제목내용"]
        lines = lines.dropna().tolist()

        for line in lines:
            line_str = str(line)
            self.tagging(line_str)

        word_file = os.path.join(self.data_dir, paths.WORD_DIRNAME,
                                 f"{str(timeline.day)}{word_file_name}")
        # FIXME(pixelhowl): async writing
        with open(word_file, "w", encoding=strings.ENCODER) as f:
            json.dump(self.tags.copy(), f, ensure_ascii=False)

        self.tags = collections.defaultdict(int)

    def tagging_with_timelines(self, df, time_conds, word_file_names,
                               timeline):
        for time_cond, word_file_name in zip(time_conds, word_file_names):
            self.tagging_with_timeline(df, time_cond, word_file_name, timeline)

    def get_total_word_dict(self):
        words = []
        last_day = calendar.monthrange(self.year, self.month)[1]

        for beg in range(1, last_day + 1):
            day_file_path = os.path.join(
                self.data_dir, paths.WORD_DIRNAME,
                f"{str(beg)}{paths.WORD_DAY_FILENAME}")
            night_file_path = os.path.join(
                self.data_dir, paths.WORD_DIRNAME,
                f"{str(beg)}{paths.WORD_NIGHT_FILENAME}")

            with open(day_file_path, encoding=strings.ENCODER) as f:
                json_file = dict(json.load(f))
                words.append(json_file)

            with open(night_file_path, encoding=strings.ENCODER) as f:
                json_file = dict(json.load(f))
                words.append(json_file)

        counter = collections.Counter()

        for tag in words:
            counter.update(tag)

        words_dict = dict(counter)
        return words_dict

    def run(self):
        content_df = database.get_content_df(self.year, self.month)
        content_df[strings.DATE_ROWNAME] = pd.to_datetime(
            content_df[strings.DATE_ROWNAME], format="%Y.%m.%d %H:%M")

        first_timeline = f"{self.year}-{self.month}"
        last_timeline = f"{self.year}-{self.month+1}" if self.month != 12 else f"{self.year+1}-1"

        for cur_timeline in tqdm(pd.date_range(first_timeline,
                                               last_timeline,
                                               inclusive="left",
                                               freq="D"),
                                 desc="Word counting"):
            day_time = cur_timeline
            afternoon_time = cur_timeline + datetime.timedelta(hours=12)
            night_time = cur_timeline + datetime.timedelta(hours=24)

            day_to_night_cond = (content_df[strings.DATE_ROWNAME] >=
                                 day_time) & (content_df[strings.DATE_ROWNAME]
                                              < night_time)

            time_cond_df = content_df[day_to_night_cond]

            day_to_afternoon_cond = (
                time_cond_df[strings.DATE_ROWNAME] >= day_time) & (
                    time_cond_df[strings.DATE_ROWNAME] < afternoon_time)
            afternoon_to_night_cond = (
                time_cond_df[strings.DATE_ROWNAME] >= afternoon_time) & (
                    time_cond_df[strings.DATE_ROWNAME] < night_time)
            time_conds = [day_to_afternoon_cond, afternoon_to_night_cond]
            file_names = [paths.WORD_DAY_FILENAME, paths.WORD_NIGHT_FILENAME]
            self.tagging_with_timelines(time_cond_df, time_conds, file_names,
                                        cur_timeline)

        vtbuer_post_comment_file = os.path.join(self.data_dir,
                                                "total_post_count.csv")
        vtbuer_post_comment_df = pd.DataFrame(
            self.vtuber_post_comment_counter.items(),
            columns=["Vtuber", "글/댓글 수"])
        vtbuer_post_comment_df.sort_values(by=["글/댓글 수"],
                                           ascending=False,
                                           inplace=True)
        result = vtbuer_post_comment_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(vtbuer_post_comment_file)

        tags_dict = self.get_total_word_dict()
        tags_df = pd.DataFrame(tags_dict.items(), columns=["단어", "언급 수"])
        tags_df = tags_df[~tags_df["단어"].isin(word_dict.WORD_FILTER)]
        tags_df.sort_values(by=["언급 수"], ascending=False, inplace=True)
        result = tags_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(self.tags_df_file)
        return tags_dict


class YoutubeCounter():

    def __init__(self, *, year=None, month=None):
        self.set_data_dir(year, month)
        self.columns = ["ChannelName", "VideoID", "Count"]
        ray.init()

    def set_data_dir(self, year, month):
        self.data_dir = database.get_data_dir(year, month)

    def yotube_rank(self):
        content_df = database.get_content_df()
        youtube_df = content_df[content_df["제목내용"].str.contains(
            strings.YOUTUBE_REGEX, na=False, regex=True)][["제목내용"]]

        yt_regex_id = ray.put(strings.YOUTUBE_REGEX)
        youtube_rank_file = os.path.join(self.data_dir, paths.YOUTUBE_FILENAME)

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
            if query.hostname == "youtu.be":
                return query.path[1:]
            if query.hostname in strings.YOUTUBE_URL_TUPLE:
                if query.path == "/watch?app=desktop":
                    p = query.path.split()
                if query.path == "/watch":
                    p = urlparse.parse_qs(query.query)
                    if "v" not in p:
                        print(value)
                        print(p)
                    return p["v"][0]
                if query.path[:7] == "/embed/":
                    return query.path.split("/")[2]
                if query.path[:3] == "/v/":
                    return query.path.split("/")[2]
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
        else:
            result_ids.append(parse_contents.remote(contents, yt_regex_id))

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

        yt_df.sort_values(by=["Count"], ascending=False, inplace=True)
        yt_df.to_csv(youtube_rank_file, sep=",")

    def run(self):
        self.yotube_rank()
        logging.LOGGER.info("yotube_rank finished.")


class WordRanker():

    def __init__(self, *, year=None, month=None, use_cache=True):
        self.use_cache = use_cache
        self.set_word_dict(year, month)

    def set_word_dict(self, year, month):
        self.year, self.month = database.get_year_month(year, month)
        self.data_dir = database.get_data_dir(self.year, self.month)
        self.prev_data_dir = database.get_data_dir(self.year,
                                                   self.month,
                                                   return_prev=True)

        self.post_file = os.path.join(self.data_dir, paths.POST_FILENAME)
        self.comment_file = os.path.join(self.data_dir, paths.COMMENT_FILENAME)
        self.tags_df_file = os.path.join(self.data_dir,
                                         paths.TOTALTAGS_FILENAME)

        if self.use_cache and os.path.isfile(self.tags_df_file):
            logging.LOGGER.info("Using cached tags...")
            tags_df = pd.read_csv(self.tags_df_file, index_col=0)
            self.word_dict = tags_df.set_index("단어").to_dict()["언급 수"]
        else:
            logging.LOGGER.info("Generate with cotent_counter...")
            self.content_counter = ContentCounter(self.tags_df_file,
                                                  year=self.year,
                                                  month=self.month)
            self.word_dict = self.content_counter.run()

    def word_cloud(self):
        for word in word_dict.WORD_FILTER:
            self.word_dict.pop(word, None)

        maskfile = np.array(PIL.Image.open(paths.WORDCLOUD_FILE))
        wc = wordcloud.WordCloud(font_path=paths.FONT_FILE,
                                 mode="RGBA",
                                 background_color=None,
                                 max_words=1000,
                                 mask=maskfile,
                                 prefer_horizontal=1,
                                 min_font_size=0,
                                 max_font_size=150,
                                 width=1398.425197,
                                 height=2116.535433,
                                 scale=5).generate_from_frequencies(
                                     self.word_dict)
        image_colors = wordcloud.ImageColorGenerator(maskfile)
        wc.recolor(color_func=image_colors, random_state=0).to_file(
            f"{self.data_dir}/{paths.WORDCLOUD_SAVE_FILENAME}")
        logging.LOGGER.info("Word cloud generated.")

    def dccon_downloader(self, rank, dccon_url):
        response = SESSION.get(dccon_url)
        dccon_filename = f"{rank}.gif"
        dccon_file = os.path.join(self.data_dir, paths.DCCON_DIRNAME,
                                  dccon_filename)
        with open(dccon_file, "wb") as f:
            f.write(response.content)
            f.close()
        response.close()

    def dccon_rank(self):
        comment_df = pd.read_json(self.comment_file)
        comment_df = comment_df[comment_df["dccon"] == True]
        dccons_counter = comment_df["content"].value_counts()

        dccon_count_file = os.path.join(self.data_dir,
                                        paths.DCCONRANK_FILENAME)
        dccon_count_df = pd.DataFrame(dccons_counter.items(),
                                      columns=["디시콘", "사용 수"])
        dccon_count_df.sort_values(by=["사용 수"], ascending=False, inplace=True)
        result = dccon_count_df.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(dccon_count_file)
        logging.LOGGER.info("dccon_rank finished.")

    def dccon_download(self):
        dccon_count_file = os.path.join(self.data_dir,
                                        paths.DCCONRANK_FILENAME)
        dccon_df = pd.read_csv(dccon_count_file, index_col=0)

        for idx, row in dccon_df.head(n=10).iterrows():
            rank = idx
            dccon_url = row["디시콘"]
            self.dccon_downloader(rank, dccon_url)

        logging.LOGGER.info("dccon_download finished.")

    def word_rank(self, vtuber_name_dict, output_filename):
        log = []
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        writer_path = os.path.join(self.data_dir, output_filename)

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
            old_data = pd.read_csv(
                os.path.join(self.prev_data_dir, output_filename))
        except:
            old_data = None

        standard = new_data.iat[0, 2]
        barlength = BAR_MAX_NUM / standard
        logging.LOGGER.debug(f"new_data: {new_data}")
        for i in range(1, TOP_NUM):
            logging.LOGGER.debug(
                f"{i+1}번째 바 길이: {new_data.iat[i,2]*barlength}")
        if old_data is not None:
            out_vtuber_df = old_data.head(n=TOP_NUM)[~old_data["Vtuber"].head(
                n=TOP_NUM).isin(new_data["Vtuber"].head(n=TOP_NUM))]
            out_vtuber_list = new_data[new_data["Vtuber"].isin(
                out_vtuber_df["Vtuber"])]
            logging.LOGGER.info(f"Top{TOP_NUM}에서 사라진: {out_vtuber_list}")

        compared_ranks = []
        for idx, member in new_data.iterrows():
            vtuber = member["Vtuber"]
            if old_data is not None and any(old_data["Vtuber"] == vtuber):
                old_row = old_data[old_data["Vtuber"] == vtuber]
                old_rank = int(old_row["Unnamed: 0"])
            else:
                old_rank = "NA"

            if old_rank == "NA":
                compared_rank = "NEW"
            elif idx > old_rank:
                compared_rank = f"▼{idx-old_rank}"
            elif idx < old_rank:
                compared_rank = f"▲{old_rank-idx}"
            elif idx == old_rank:
                compared_rank = "■-"
            else:
                compared_rank = "Error"
            compared_ranks.append(compared_rank)

        new_data["전월 대비 순위"] = compared_ranks
        result = new_data.reset_index(drop=True)
        result.index = result.index + 1
        result.to_csv(writer_path, sep=",", index=True)

    def run(self):
        self.word_rank(vtuber_dict.nijisanji, paths.NIJIRANK_FILENAME)
        self.word_rank(vtuber_dict.hololive, paths.HOLORANK_FILENAME)
        self.word_rank(vtuber_dict.vtuber, paths.VTUBERRANK_FILENAME)
        self.word_rank(vtuber_dict.exceptholo, paths.NOHOLORANK_FILENAME)
        logging.LOGGER.info("word_rank finished.")


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
    logging.LOGGER.debug("Get app_id=%s" % app_id)
    return app_id


def get_html(page_num, app_id, post_list, comment_list):  #글 수집
    post_encode_url = strings.DCINSIDE_PAGE_URL.format(page_num=page_num,
                                                       app_id=app_id)
    page_url = strings.DCINSIDE_MOBILE_URL.format(
        hash=interpret_url(post_encode_url))

    for trial_count in range(SESSION_RETRIAL_COUNT):
        try:
            post_page = SESSION.get(page_url,
                                    headers=strings.DCINSIDE_HEADER,
                                    timeout=SESSION_TIMEOUT)
            intro_json = post_page.json(strict=False)[0]
            view_json = post_page.json(strict=False)[0]
            if strings.DELETE_URL in post_page.text or "글없음" in post_page.text or "view_info" not in intro_json:
                post_page.close()
                post_list.append({
                    "번호": page_num,
                    "제목": None,
                    strings.DATE_ROWNAME: None,
                    "닉네임": None,
                    "ID/IP": None,
                    "idtype": None,
                    "조회 수": 0,
                    "달린 댓글 수": 0,
                    "추천 수": 0,
                    "비추 수": 0,
                    "content": None,
                    "mobile": None,
                    "개념글 수": None
                })
                return
            intro = intro_json["view_info"]
            view = view_json["view_main"]
            #글 내용
            post_content = view["memo"]
            #작성자 IP
            post_ip = intro["user_id"] if len(
                intro["ip"]) == 0 else intro["ip"]
            #모바일 체크
            mobile = intro["write_type"] != "W"
            #개념글 체크
            recom = intro["recommend_chk"] != "N"
            post_page.close()
            break
        except (requests.exceptions.RequestException, requests.Timeout) as e:
            pass
        except Exception as e:
            if "NotFound" in post_page.text and "404" in post_page.text:
                logging.LOGGER.info(
                    f"Post Trial: {trial_count}, break with 404 NotFound Error, content: {post_page.text}"
                )
                post_list.append({
                    "번호": page_num,
                    "제목": None,
                    strings.DATE_ROWNAME: None,
                    "닉네임": None,
                    "ID/IP": None,
                    "idtype": None,
                    "조회 수": 0,
                    "달린 댓글 수": 0,
                    "추천 수": 0,
                    "비추 수": 0,
                    "content": None,
                    "mobile": None,
                    "개념글 수": None
                })
                return
            if not ("<!DOCTYPE html>" in post_page.text
                    or post_page.text == ""):
                logging.LOGGER.info(
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
        comment_encode_url = strings.DCINSIDE_COMMENT_URL.format(
            page_num=page_num, reply_page=reply_page, app_id=app_id)
        mobile_url = strings.DCINISDE_MOBILE_COMMENT_URL.format(
            hash=interpret_url(comment_encode_url))
        pass_nickname = pass_ip = "deleted"
        for trial_count in range(SESSION_RETRIAL_COUNT):
            try:
                comment_page = SESSION.get(mobile_url,
                                           headers=strings.DCINSIDE_HEADER,
                                           timeout=5)
                comment = comment_page.json(strict=False)[0]
            except (requests.exceptions.RequestException,
                    requests.Timeout) as e:
                time.sleep(1)
                continue
            except Exception as e:
                if "\"message\":\"Not Found\"" in comment_page.text and "\"status\":404" in comment_page.text:
                    logging.LOGGER.info(
                        f"Comment Trial: {trial_count}, post_number: {page_num}, break with 404 NotFound Error, content: {comment_page.text}"
                    )

                if not ("<!DOCTYPE html>" in comment_page.text
                        or comment_page.text == ""):
                    logging.LOGGER.info(
                        f"Comment Trial: {trial_count}, post_number: {page_num} , error: {str(e)}, content: {comment_page.text}"
                    )

                comment_page.close()
                break

            if "total_page" not in comment:
                break

            total_page = int(comment["total_page"])

            for comm in comment["comment_list"]:
                has_ip_data = "ipData" in comm
                id_or_ip = comm["ipData"] if has_ip_data and len(
                    comm["ipData"]) else comm["user_id"]

                has_dccon = "dccon" in comm
                removed_by_writer = "is_delete_flag" in comm and "작성자" in comm[
                    "is_delete_flag"]

                if "under_step" in comm:
                    target = f"{pass_nickname} ({pass_ip})"
                else:
                    if removed_by_writer:
                        pass_nickname = pass_ip = "deleted"
                    else:
                        pass_nickname = comm["name"]
                        pass_ip = id_or_ip
                    target = ""

                content = comm["comment_memo"] if not has_dccon else comm[
                    "dccon"]

                comment_list.append({
                    "번호": int(page_num),
                    strings.DATE_ROWNAME: comm["date_time"],
                    "닉네임": comm["name"],
                    "ID/IP": id_or_ip,
                    "idtype": comm["member_icon"],
                    "content": content,
                    "dccon": has_dccon,
                    "답글 대상": target,
                    "댓삭 당한 횟수": removed_by_writer
                })
            comment_page.close()
            break

        if "comment_list" not in comment:
            break
        total_comment += len(comment["comment_list"])
        reply_page += 1

    post_list.append({
        "번호": page_num,
        "제목": intro["subject"],
        strings.DATE_ROWNAME: intro["date_time"],
        "닉네임": intro["name"],
        "ID/IP": post_ip,
        "idtype": intro["member_icon"],
        "조회 수": intro["hit"],
        "달린 댓글 수": total_comment,
        "추천 수": view["recommend"],
        "비추 수": view["nonrecommend"],
        "content": post_content,
        "mobile": mobile,
        "개념글 수": recom
    })


def get_prev_month_last_page(prev_data_dir=None):
    if prev_data_dir is None:
        prev_data_dir = database.get_data_dir(return_prev=True)
    prev_post_file = os.path.join(prev_data_dir, paths.POST_FILENAME)
    post_df = pd.read_json(prev_post_file)
    if len(post_df):
        last_page = post_df["번호"].max()
    else:
        raise ValueError(
            "Cannot find last page with nothing in {prev_post_file}")
    return last_page


def run_web_crawler(start_page_num, end_page_num):
    if start_page_num is None:
        start_page_num = get_prev_month_last_page()
    dup_check_df = pd.read_csv(paths.DUPCHECK_FILE, index_col=None).astype(int)
    logging.LOGGER.info("Get Auth class from KotlinInside.")
    auth = kotilnside.get_auth()
    logging.LOGGER.info("Create gevent pool.")
    pool = gevent.pool.Pool()

    app_id = get_app_id(auth)

    for cur_page_num in tqdm(range(start_page_num, end_page_num,
                                   TRIAL_WITH_APP_ID),
                             desc=f"{strings.GALLARY_NAME} Webcrawling..."):
        post_list = []
        comment_list = []

        next_page_num = cur_page_num + TRIAL_WITH_APP_ID
        if next_page_num > end_page_num:
            next_page_num = end_page_num + 1

        for page_num in range(cur_page_num, next_page_num):
            if page_num in dup_check_df["번호"].values:
                logging.LOGGER.debug(f"{page_num} exists so skip.")
                continue
            pool.spawn(get_html, page_num, app_id, post_list, comment_list)

        pool.join()

        if len(post_list) > 0:
            logging.LOGGER.info("Process %s posts", len(post_list))  # 수집 글 갯수
            app_id = get_app_id(auth)

        database.update_and_save_df(paths.POST_FILENAME, post_list)
        database.update_and_save_df(paths.COMMENT_FILENAME, comment_list)
