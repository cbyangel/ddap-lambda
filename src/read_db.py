import os as _os

import pandas as _pd
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = _os.getenv('PROJECT_HOME')

def get_svd(conn, broad_dt, is_catv):
    if is_catv:
        chanl_cd, mnfc_gbn_cd = 'C', '1'
    else:
        chanl_cd, mnfc_gbn_cd = 'H', '5'

    with open(_os.path.join(ROOT_DIR, 'sql', 'svd.sql'), 'r') as f:
        sql = f.read()

    params = {'broad_dt': broad_dt, 'chanl_cd': chanl_cd, 'mnfc_gbn_cd': mnfc_gbn_cd}
    svd_data = _pd.read_sql(sql, conn, params=params)
    return svd_data


def get_view(conn, broad_dt, is_catv):
    """
    :param conn: oracle db connection
    :param broad_dt: 방송일자
    :param is_catv: 라이브방송 가져올 경우 True 데이터 방송일 경우 False
    :return: ddap bi report 테이블 생성
    상품군 필터링하여 의류/속옷만 가져옴
    """

    if is_catv:
        chanl_cd, mnfc_gbn_cd = 'C', '1'
    else:
        chanl_cd, mnfc_gbn_cd = 'H', '5'

    with open(_os.path.join(ROOT_DIR, 'sql', 'view_base.sql'), 'r') as f:
        sql = f.read()

    params = {'broad_dt': broad_dt, 'chanl_cd': chanl_cd, 'mnfc_gbn_cd': mnfc_gbn_cd}
    view_base = _pd.read_sql(sql, conn, params=params)
    return view_base
