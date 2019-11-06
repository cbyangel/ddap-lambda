import dotenv

_env_path = dotenv.find_dotenv(raise_error_if_not_found=True)
assert _env_path != '', 'failed to load environment variables'
dotenv.load_dotenv(_env_path)

from time import ctime

import pandas as pd
from src import write_db
from src import read_db
from src import dbutil
from src import data_process


def main(broad_dt):
    for is_catv in [True, False]:
        print('starting python {} {}'.format(broad_dt, is_catv))

        ora_session = dbutil.dhub()
        conn = ora_session.acquire()

        """
        get view dataset
        """

        print('{}\tpresentation dataset'.format(ctime()))
        view = read_db.get_view(conn, broad_dt, is_catv)
        view = data_process.prepare_columns(view)
        view = data_process.astype_view(view)
        view = data_process.set_index(view)

        """
        get svd dataset
        """

        print('{}\tquery svd dataset'.format(ctime()))
        svd = read_db.get_svd(conn=conn, broad_dt=broad_dt, is_catv=is_catv)
        svd = data_process.astype_svd(svd)
        split_df = data_process.split_svd_format(filtered_svd_data=svd)

        """
        detect attribute sold out and replace null
        and impute null with svd (same as pmf) 
        """

        print('{}\tassort imputation'.format(ctime()))

        view_ = pd.DataFrame()
        sold_out_snapshot_ = pd.DataFrame()
        for key in split_df.keys():
            order = split_df[key]
            pgm_id, prd_cd = key
            view_part = view.query('PGM_ID == @pgm_id and PRD_CD == @prd_cd').copy()
            order_ = data_process.add_no_ordered_attr(order, view_part)
            order_with_na = data_process.make_na_when_sold_out(view_part, order_, is_catv)
            svd_imputed = data_process.svd_impute(order_with_na)
            assert svd_imputed.shape[0] == order_.shape[0], 'svd_impute has wrong dimension'
            if view_part.shape[0] != svd_imputed.shape[0]:
                continue

            data_process.add_estim_qty(view_part, svd_imputed)
            data_process.add_sold_out_dtm(view_part, order_with_na)
            sold_out_snapshot = data_process.get_snapshot_each_sold_out(view_part, order_with_na)
            data_process.add_first_sold_out_snapshot(view_part, sold_out_snapshot)
            data_process.add_pgm_time_string(view_part)
            data_process.add_attribute_detail(view_part)
            view_part = data_process.fill_blank_order(view_part)
            view_part = data_process.column_translate(view_part)

            sold_out_snapshot = sold_out_snapshot.assign(PGM_ID=pgm_id, BROAD_DT=broad_dt)
            assert sold_out_snapshot.stack().isna().sum() == 0, ('snapshot has unexpected null', sold_out_snapshot)
            view_ = view_.append(view_part)
            sold_out_snapshot_ = sold_out_snapshot_.append(sold_out_snapshot)

        """write df on dhub"""

        write_db.write_auto_db(view_)
        write_db.write_snapshot_db(sold_out_snapshot_)

        print('{}\tdhub inserted'.format(ctime()))


if __name__ == '__main__':
    broad_dt = 20160808
    write_db.delete_by_day_auto_table(broad_dt)
    write_db.delete_by_day_snapshot_table(broad_dt)
    main(broad_dt)
