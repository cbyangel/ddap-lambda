from src.dbutil import ddap

TEST_TABLE_NAME = 'ddap_report_auto_beta'
BI_TABLE_NAME = 'ddap_report_auto'
SNAPSHOT_TABLE_NAME = 'ddap_soldout_snapshot_auto'


def write_auto_db(view):
    dta = ddap()
    view.to_sql(name=BI_TABLE_NAME, schema='dta_own',
                con=dta, index=False, if_exists='append')
    return


def write_test_db(view):
    dta = ddap()
    view.to_sql(name=TEST_TABLE_NAME, schema='dta_own',
                con=dta, index=False, if_exists='replace')
    return


def delete_by_day_snapshot_table(day: str):
    dta = ddap()
    sql = """delete from {} where BROAD_DT = '{}'""".format(SNAPSHOT_TABLE_NAME, day)
    dta.execute(sql)


def delete_by_day_auto_table(day: str):
    dta = ddap()
    sql = """delete from {} where 방송일자 = '{}'""".format(BI_TABLE_NAME, day)
    dta.execute(sql)


def drop_test_table():
    dta = ddap()
    sql = """DROP TABLE {}""".format(TEST_TABLE_NAME)
    dta.execute(sql)


def write_snapshot_db(sold_out_snapshot):
    dta = ddap()
    sold_out_snapshot.to_sql(name=SNAPSHOT_TABLE_NAME, schema='dta_own',
                             con=dta, index=False, if_exists='append')
    return