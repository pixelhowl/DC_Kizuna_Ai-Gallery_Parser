"""Save releated"""
import datetime
import os

import pandas as pd
from tqdm.notebook import tqdm

from . import logging, paths, strings

# pylint: disable=singleton-comparison

DUPCHECK_COL_NAME = ["번호"]
POST_COL_NAME = {
    "번호": int,
    "제목": str,
    strings.DATE_ROWNAME: str,
    "닉네임": str,
    "ID/IP": str,
    "조회 수": int,
    "달린 댓글 수": int,
    "추천 수": int,
    "비추 수": int,
    "content": str,
    "mobile": bool,
    "개념글 수": bool,
    "idtype": str
}
COMMENT_COL_NAME = {
    "번호": int,
    strings.DATE_ROWNAME: str,
    "닉네임": str,
    "ID/IP": str,
    "dccon": bool,
    "content": str,
    "idtype": str,
    "답글 대상": str,
    "댓삭 당한 횟수": bool
}


def get_year_month(in_year=None, in_month=None):
    year = datetime.datetime.now().year if in_year is None else int(in_year)
    month = datetime.datetime.now().month - 1 if in_month is None else int(
        in_month)
    return year, month


def get_prev_year_month(in_year=None, in_month=None):
    year, month = get_year_month(in_year, in_month)

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1

    return prev_year, prev_month


def get_col_and_dtype(filename):
    if filename == paths.POST_FILENAME:
        return POST_COL_NAME
    if filename == paths.COMMENT_FILENAME:
        return COMMENT_COL_NAME
    raise ValueError(f"There is no col with {filename}")


def remove_duplicated_in_df(year=None, month=None):
    post_file = get_df_path(paths.POST_FILENAME, year=year, month=month)
    _ = get_df_path(paths.POST_FILENAME, year=year, month=month)
    post_df, _ = get_post_and_content_df(year, month)
    post_df.drop_duplicates(["번호"]).to_json(post_file, force_ascii=False)
    # comment_df.drop_duplicates(["번호"]).to_json(comment_file,
    #                                            force_ascii=False)


def post_processing_df():
    pass
    # remove_html_content_in_df()
    # remove_duplicated_in_df(year, month)


def get_data_dir(in_year=None, in_month=None, *, return_prev=False):
    year_func = get_prev_year_month if return_prev else get_year_month
    year, month = year_func(in_year, in_month)
    data_dir = f"{paths.DB_PATH}/{year}/{month}/"
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def get_df_path(filename, data_dir=None, year=None, month=None):
    data_dir = get_data_dir(year, month) if data_dir is None else data_dir
    os.makedirs(data_dir, exist_ok=True)
    assert filename is not None
    df_path = os.path.join(data_dir, filename)
    return df_path


def create_df(df_path):
    if not os.path.isfile(df_path):
        data_dir, filename = os.path.split(df_path)
        os.makedirs(data_dir, exist_ok=True)
        col_and_dtype = get_col_and_dtype(filename)
        pd.DataFrame(
            columns=col_and_dtype.keys()).astype(col_and_dtype).to_json(
                df_path, force_ascii=False)


def try_get_df(df_path):
    create_df(df_path)
    return pd.read_json(df_path)


def get_df(df_path=None, filename=None, data_dir=None, year=None, month=None):
    df_path = get_df_path(filename, data_dir, year,
                          month) if df_path is None else df_path
    df = try_get_df(df_path)
    return df


def get_post_and_content_df(year=None, month=None):
    post_df = get_df(filename=paths.POST_FILENAME, year=year, month=month)
    comment_df = get_df(filename=paths.COMMENT_FILENAME,
                        year=year,
                        month=month)
    return post_df, comment_df


def get_content_df(year=None, month=None):
    post_df, comment_df = get_post_and_content_df(year, month)

    comment_df = comment_df[comment_df["dccon"] == False]
    content_df = pd.concat([post_df, comment_df], sort=False)
    content_df = remove_html_content_in_df(content_df, "제목내용")
    content_df["제목내용"] = content_df["제목"].astype(
        str) + "\n" + content_df["content"].astype(str)
    return content_df


def generate_db(start_year=2021, end_year=2022):
    pd.DataFrame(columns=DUPCHECK_COL_NAME,
                 index=None).to_csv(paths.DUPCHECK_FILE, index=False, mode="w")

    # 년도 폴더 생성
    for year in tqdm(range(start_year, end_year + 1),
                     desc="Building based on year"):
        #1~12월 폴더 생성,
        for month in tqdm(range(1, 12 + 1), desc="Building based on month"):
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

            post_file = get_df_path(paths.POST_FILENAME, data_dir=base_dir)
            comment_file = get_df_path(paths.COMMENT_FILENAME,
                                       data_dir=base_dir)

            post_df = try_get_df(post_file)
            create_df(comment_file)
            post_df["번호"].drop_duplicates().to_csv(paths.DUPCHECK_FILE,
                                                   index=False,
                                                   mode="a",
                                                   header=False)
    # 0 = 삭제된 글거나 없어진 글들
    base_dir = get_data_dir(0, 0)
    post_file = get_df_path(paths.POST_FILENAME, data_dir=base_dir)
    post_df = try_get_df(post_file)
    post_df["번호"].drop_duplicates().to_csv(paths.DUPCHECK_FILE,
                                           index=False,
                                           mode="a",
                                           header=False)


def remove_and_save_html_content_in_post_df(year, month):
    post_file = get_df_path(paths.POST_FILENAME, year=year, month=month)
    post_df = get_df(df_path=post_file)
    remove_html_content_in_df(post_df, strings.CONTENT_ROWNAME)
    post_df.to_json(post_file, force_ascii=False)


def remove_html_content_in_df(dataframe, row_name):
    dataframe[row_name].astype(str)
    dataframe[row_name] = dataframe[row_name].str.replace(r"<[^<]+?>",
                                                          "",
                                                          regex=True)
    dataframe[row_name] = dataframe[row_name].str.replace(r"(&lt;).*?(&gt;)",
                                                          "",
                                                          regex=True)
    return dataframe


def update_and_save_df(filename, row_list):
    if len(row_list) > 0:
        dtype = get_col_and_dtype(filename)
        update_df = pd.DataFrame(row_list)
        update_df.astype(dtype=dtype, copy=False)
        update_df["year_month"] = update_df[strings.DATE_ROWNAME].str.slice(
            stop=7)
        year_months = update_df["year_month"].unique()
        for year_month in year_months:
            if year_month is None:
                year = month = 0
            else:
                year, month = year_month.split(".")
            df_path = get_df_path(filename, year=year, month=month)
            df = get_df(df_path=df_path).astype(dtype)
            new_df = update_df[update_df["year_month"] == year_month].drop(
                columns="year_month")
            pd.concat([df, new_df], sort=False,
                      ignore_index=True).to_json(df_path, force_ascii=False)


def test_db(start_year=2022, end_year=2022):
    for year in tqdm(range(start_year, end_year + 1),
                     desc="Test based on year"):  # 년도 폴더 생성
        for month in tqdm(range(1, 12 + 1),
                          desc="Test based on month"):  #1~12월 폴더 생성
            base_dir = get_data_dir(year, month)

            post_file = get_df_path(paths.POST_FILENAME, data_dir=base_dir)
            comment_file = get_df_path(paths.COMMENT_FILENAME,
                                       data_dir=base_dir)

            post_df = try_get_df(post_file)
            comment_df = try_get_df(comment_file)

            logging.LOGGER.info("paths: %s", base_dir)
            logging.LOGGER.info("post: %s, comment: %s", post_df.head(1),
                                comment_df.head(1))
