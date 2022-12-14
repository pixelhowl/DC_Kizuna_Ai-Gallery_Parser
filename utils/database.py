"""Save releated"""
import datetime
import os

import pandas as pd

from . import parsers, paths, strings

# pylint: disable=singleton-comparison

POST_COL_NAME = [
    "번호", "제목", "날짜", "닉네임", "ID/IP", "조회 수", "달린 댓글 수", "추천 수", "비추 수",
    "content", "mobile", "개념글 수", "idtype", "dccon"
]
COMMENT_COL_NAME = [
    "번호", "날짜", "닉네임", "ID/IP", "dccon", "content", "idtype", "답글 대상",
    "댓삭 당한 횟수"
]


def get_year_month(in_year=None, in_month=None):
    year = datetime.datetime.now().year if in_year is None else in_year
    month = datetime.datetime.now().month - 1 if in_month is None else in_month
    return year, month


def get_prev_year_month(in_year=None, in_month=None):
    year, month = get_year_month(in_year, in_month)

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1

    return prev_year, prev_month


def get_data_dir(in_year=None, in_month=None, *, return_prev=False):
    year_func = get_prev_year_month if return_prev else get_year_month
    year, month = year_func(in_year, in_month)
    return f"{paths.HOME}/{strings.GALLARY_NAME}/{year}/{month}/"


def get_post_df():
    post_df = pd.read_json(parsers.POST_FILE)
    return post_df


def get_comment_df():
    comment_df = pd.read_json(parsers.COMMENT_FILE)
    return comment_df


def get_post_and_content_df():
    post_df = pd.read_json(parsers.POST_FILE)
    comment_df = pd.read_json(parsers.COMMENT_FILE)
    return post_df, comment_df


def generate_db_directory(start_year=2022, end_year=2022):
    for year in range(start_year, end_year + 1):  # 년도 폴더 생성
        for month in range(1, 12 + 1):  #1~12월 폴더 생성
            base_dir = get_data_dir(year, month)
            table_dir = os.path.join(base_dir, paths.TABLE_DIRNAME)
            word_dir = os.path.join(base_dir, paths.WORD_DIRNAME)
            dccon_dir = os.path.join(base_dir, paths.DCCON_DIRNAME)
            video_thumbnail_dir = os.path.join(base_dir,
                                               paths.THUMBNAIL_DIRNAME)

            os.makedirs(base_dir, exist_ok=True)
            os.makedirs(table_dir, exist_ok=True)
            os.makedirs(word_dir, exist_ok=True)
            os.makedirs(dccon_dir, exist_ok=True)
            os.makedirs(video_thumbnail_dir, exist_ok=True)

            if not os.path.isfile(parsers.POST_FILE):
                pd.DataFrame(columns=POST_COL_NAME).to_json(parsers.POST_FILE,
                                                            force_ascii=False)
            if not os.path.isfile(parsers.COMMENT_FILE):
                pd.DataFrame(columns=COMMENT_COL_NAME).to_json(
                    parsers.COMMENT_FILE, force_ascii=False)


def remove_and_save_html_content_in_post_df():
    post_df = pd.read_json(parsers.POST_FILE)
    remove_html_content_in_df(post_df, strings.CONTENT_ROWNAME)
    post_df.to_json(parsers.POST_FILE, force_ascii=False)


def remove_html_content_in_df(dataframe, row_name):
    dataframe[row_name].astype(str)
    dataframe[row_name] = dataframe[row_name].str.replace(r"<[^<]+?>",
                                                          "",
                                                          regex=True)
    dataframe[row_name] = dataframe[row_name].str.replace(r"(&lt;).*?(&gt;)",
                                                          "",
                                                          regex=True)
    return dataframe


def remove_duplicated_in_df():
    post_df, comment_df = get_post_and_content_df()
    post_df.drop_duplicates(["번호"]).to_json(parsers.POST_FILE,
                                            force_ascii=False)
    comment_df.drop_duplicates(["번호"]).to_json(parsers.COMMENT_FILE,
                                               force_ascii=False)


def post_processing_df():
    # remove_html_content_in_df()
    remove_duplicated_in_df()


def get_content_df():
    post_df, comment_df = get_post_and_content_df()

    comment_df = comment_df[comment_df["dccon"] == False]
    content_df = pd.concat([post_df, comment_df], sort=False)
    content_df["제목내용"] = content_df["제목"].astype(
        str) + "\n" + content_df["content"].astype(str)
    content_df = remove_html_content_in_df(content_df, "제목내용")
    return content_df
