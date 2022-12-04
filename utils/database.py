import os

import pandas as pd

from . import *
from . import parsers

POST_COL_NAME = [
    '번호', '제목', '날짜', '닉네임', 'ID/IP', '조회 수', '달린 댓글 수', '추천 수', '비추 수',
    'content', 'mobile', '개념글 수', 'idtype', 'dccon'
]
COMMENT_COL_NAME = [
    '번호', '날짜', '닉네임', 'ID/IP', 'dccon', 'content', 'idtype', '답글 대상',
    '댓삭 당한 횟수'
]

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
    
def generate_db_directory():
    table_str = 'table'
    word_str = 'word'
    dccon_str = 'dccon'
    thumbnail_str = 'thumbnail'

    for year in range(START_YEAR, END_YEAR + 1):  # 년도 폴더 생성
        for month in range(1, 12 + 1):  #1~12월 폴더 생성
            base_dir = os.path.abspath(
                f'{HOME}/{GALLARY_NAME}/{year}/{month}/')
            table_dir = os.path.join(base_dir, table_str)
            word_dir = os.path.join(base_dir, word_str)
            dccon_dir = os.path.join(base_dir, dccon_str)
            video_thumbnail_dir = os.path.join(base_dir, thumbnail_str)

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
    remove_html_content_in_df(post_df)
    post_df.to_json(parsers.POST_FILE, force_ascii=False)


def remove_html_content_in_df(dataframe, row_name):
    dataframe[row_name].astype(str)
    dataframe[row_name] = dataframe[row_name].str.replace(r'<[^<]+?>',
                                                        '',
                                                        regex=True)
    dataframe[row_name] = dataframe[row_name].str.replace(r'(&lt;).*?(&gt;)',
                                                        '',
                                                        regex=True)
    return dataframe


def remove_duplicated_in_df():
    post_df, comment_df = get_post_and_content_df()
    post_df.drop_duplicates(['번호']).to_json(parsers.POST_FILE,
                                            force_ascii=False)
    comment_df.drop_duplicates(['번호']).to_json(parsers.COMMENT_FILE,
                                               force_ascii=False)


def post_processing_df():
    # remove_html_content_in_df()
    remove_duplicated_in_df()
    
def get_content_df():
    post_df, comment_df = get_post_and_content_df()
    comment_df = comment_df[comment_df['dccon'] == False]
    content_df = pd.concat([post_df, comment_df], sort=False)
    content_df["제목내용"] = content_df[u'제목'].astype(
        str) + '\n' + content_df['content'].astype(str)
    content_df = remove_html_content_in_df(content_df, "제목내용")
    return content_df
