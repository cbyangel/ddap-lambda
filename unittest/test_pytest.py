"""

This code is deprecated
May used for exmaple

"""

import random

import pandas as pd
import pytest

import src.read_db
from src import data_process as dat

n_test = 100

test_arg_1 = [int(x) for x in pd.bdate_range(start='2017-01-01', end='2019-06-01').strftime('%Y%m%d')]
test_arg_2 = [True, True, False]
pytest_args = tuple(zip(random.choices(test_arg_1, k=n_test),
                        random.choices(test_arg_2, k=n_test)))


@pytest.fixture(scope='module')
def dhub_session():
    from src.dbutil import dhub
    session = dhub()
    yield session


@pytest.mark.parametrize("broad_dt, is_catv", pytest_args)
def test_svd(dhub_session, broad_dt, is_catv):
    con = dhub_session.acquire()
    svd_data = src.read_db.get_svd(con, broad_dt, is_catv)
    split_df = dat.svd_data_split_by_pgm_prd(filtered_svd_data=svd_data)
    for i in range(len(split_df)):
        each = split_df[i]
        key, order = each['key'], each['order']
        supply = dat.get_supply_of_pgm(con, key)
        order_for_svd = dat.make_na_when_sold_out(supply, order)
        order_for_svd_drop = dat.deprecated_drop_order_zero_assort(order_for_svd)
        final_out = dat.svd_impute(order_for_svd_drop)
        check_out = final_out.groupby(dat.ATTR_KEY)[dat.ORD_AMT_KEY].max()
        assert check_out.max() <= 5000
        print(check_out)
    con.close()


def test_db(dhub_session):
    import pandas as pd
    con = dhub_session.acquire()
    out = pd.read_sql('select * from d_prd_item_m where rownum < 5', con)
    print(out)
