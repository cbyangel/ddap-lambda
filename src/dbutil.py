import dotenv

_env_path = dotenv.find_dotenv(raise_error_if_not_found=True)
assert _env_path != '', 'failed to load environment variables'
dotenv.load_dotenv(_env_path)

import calendar as _calendar
import os as _os

import cx_Oracle as _ora
import pandas as _pd
import sqlalchemy as _sa



def dhub():
    """
    :return: connection pool 객체 생성
    ora_session.acquire() 로 connection 활성해야 사용 가능함
    """

    ora_id = _os.getenv('DHUB_ID')
    ora_pw = _os.getenv('DHUB_PW') % _pd.Timestamp.now().month
    ora_host = _os.getenv('ORA_HOST')
    dsn = _ora.makedsn(ora_host, 1522, service_name='DHUB1')
    ora_session = _ora.SessionPool(dsn=dsn, user=ora_id, password=ora_pw,
                                   encoding="UTF-8",
                                   threaded=True,
                                   min=1, max=20)
    return ora_session


def ddap():
    """
    :return: connection object
    cx_oracle은 insert 시 01036 error 발생
    """

    ora_id = _os.getenv('DTA_ID')
    ora_pw = _os.getenv('DTA_PW')
    ora_host = _os.getenv('ORA_HOST')
    engine = _sa.create_engine('oracle+cx_oracle://{id}:{pw}@{ip}:1522/?service_name=dhub1'
                               .format(id=ora_id, pw=ora_pw, ip=ora_host))

    return engine


def create_dates_within_month(year, month):
    start_day = 1
    _, end_day = _calendar.monthrange(year, month)

    from_dt = _pd.to_datetime('{}/{}/{}'.format(year, month, start_day), format='%Y/%m/%d')
    to_dt = _pd.to_datetime('{}/{}/{}'.format(year, month, end_day), format='%Y/%m/%d')
    dates = [i.strftime('%Y%m%d') for i in _pd.date_range(from_dt, to_dt)]
    return dates
