# edited 2019/06/24

import random
import numpy as np
from surprise import Dataset
from surprise import Reader
from surprise.prediction_algorithms.matrix_factorization import SVD
from surprise.model_selection import GridSearchCV

import pandas as pd

SEED = 23094
N_CORE = 1
BUFFER_TIME_SIZE = 3
LOW_ORDER_THRESHOLD = 5

TIME_KEY = 'ORD_TIME'
ATTR_KEY = 'ATTR_PRD_CD'
ORD_AMT_KEY = 'TOT_ORD_QTY'
PGM = 'PGM_ID'
PRD = 'PRD_CD'
DF_DIVIDE_KEY_NAME = [PGM, PRD]
SVD_KEYS = [ORD_AMT_KEY, TIME_KEY, ATTR_KEY]
SUPPLY_AMT_KEY = 'SUPLY_APNT_QTY'

WEEKDAY_EN_TO_KR = {'Sun': '일', 'Sat': '토', 'Mon': '월',
                    'Wed': '수', 'Fri': '금', 'Tue': '화', 'Thu': '목'}

COL_NAME_EN_TO_KR = {'ITEM_NM': '아이템명', 'ITEM_CD': '아이템코드',
                     'SIZE_STR': '사이즈_체계', 'COLOR_CNT': '컬러수',
                     'PRD_NM': '상품명', 'PRD_CD': '상품코드',
                     'FST_BROAD_YYYY': '런칭년도', 'FST_BROAD_MM': '런칭월',
                     'ATTR_PRD_CD': '속성코드', 'CHANL_CD': '방송채널코드',
                     'BROAD_SEQ': '방송회차', 'PGM_ID': '방송코드',
                     'ATTR_PRD_1_VAL': '컬러', 'ATTR_PRD_2_VAL_ORG': '사이즈',
                     'ATTR_PRD_2_VAL': '사이즈_변환', 'SIZE2': '사이즈2',
                     'SIZE3': '사이즈3', 'SOLD_OUT_FLAG': '품절여부',
                     'SOLD_OUT_DTM': '일시품절', 'SUPLY_APNT_QTY': '공급약속수량',
                     'PORD_03_TOT_ORD_QTY': '미리주문', 'VALUE': '첫일시품절시_수량',
                     'ESTIM_QTY': '추정_기대수요_수량', 'TOT_ORD_QTY': '실제_총주문_수량',
                     'PGM_NM': '프로그램명', 'BROAD_DT': '방송일자',
                     'BROAD_STR_DTM': '방송일자시간', 'PGM_TIME': '방송시간',
                     'TOT_ORD_AMT': '총주문_천원', 'NET_ORD_AMT': '순주문_천원',
                     'CHG_RT': '전환율', 'EXPOS_MI': '노출분',
                     'WEIHT_MI': '가중분', 'EXPCT_SAL_AMT_PER_WMI': '가중분취',
                     'ATTR_PRD_WHL_VAL': '상품속성값', 'MYSHOP_FST_BROAD_DT': '데이터런칭일자',
                     'SIZE2': '사이즈2', 'SIZE3': '사이즈3', 'MD_ID': 'MDID',
                     'FST_BROAD_DT': '라이브런칭일자', 'FST_PRD_SALE_PRC': '최초판매가'
                     }

VIEW_DTYPES = {'ATTR_PRD_1_VAL': str, 'ATTR_PRD_2_VAL': str,
               'ATTR_PRD_2_VAL_ORG': str, 'ATTR_PRD_CD': int,
               'ATTR_PRD_WHL_VAL': str, 'BROAD_DT': str,
               'BROAD_SEQ': 'float64', 'BROAD_STR_DTM': 'datetime64[ns]',
               'CHANL_CD': str, 'CHG_RT': 'float64',
               'COLOR_CNT': 'float64', 'ESTIM_QTY': 'float64',
               'EXPCT_SAL_AMT_PER_WMI': 'float64', 'EXPOS_MI': 'float64',
               'FST_BROAD_MM': 'float64', 'FST_BROAD_YYYY': 'float64',
               'ITEM_CD': 'float64', 'ITEM_NM': str,
               'MYSHOP_FST_BROAD_DT': 'float64', 'NET_ORD_AMT': 'float64',
               'PGM_ID': int, 'PGM_NM': str,
               'PGM_TIME': str, 'PORD_03_TOT_ORD_QTY': 'float64',
               'PRD_CD': int, 'PRD_NM': str,
               'SIZE2': str, 'SIZE3': str,
               'SIZE_STR': str, 'SOLD_OUT_DTM': 'datetime64[ns]',
               'SOLD_OUT_FLAG': 'float64', 'SUPLY_APNT_QTY': 'float64',
               'TOT_ORD_AMT': 'float64', 'TOT_ORD_QTY': 'float64',
               'VALUE': 'float64', 'WEIHT_MI': 'float64', 'MD_ID': str,
               'FST_BROAD_DT': 'float64', 'FST_PRD_SALE_PRC': 'float64'
               }

SVD_DTYPES = {'PGM_ID': int, 'PRD_CD': int, 'CHANL_CD': str, 'ATTR_PRD_CD': int,
              'ORD_DT': str, 'ORD_TIME': str, 'TOT_ORD_QTY': int}

COLUMNS = list(VIEW_DTYPES.keys())


def low_order_pgm_filter(svd_data):
    bools = svd_data.groupby('PGM_ID')[ORD_AMT_KEY].sum() >= LOW_ORDER_THRESHOLD
    over_threshold_pgms = bools.loc[bools].index
    filtered_data = svd_data.query('PGM_ID in @over_threshold_pgms')
    return filtered_data


def split_svd_format(filtered_svd_data):
    split_df = dict()
    svd_data_keyed = filtered_svd_data.groupby(DF_DIVIDE_KEY_NAME)
    df_divide_keys = list(filtered_svd_data[DF_DIVIDE_KEY_NAME]
                          .drop_duplicates()
                          .itertuples(index=False))
    for df_divide_key in df_divide_keys:
        # split order by pgmid/prdcd
        each_df = svd_data_keyed.get_group(df_divide_key)

        # if order is zero, no return
        if each_df.empty:
            continue

        each_df = each_df.assign(**{TIME_KEY: each_df['ORD_DT'].str.cat(each_df['ORD_TIME']).apply(pd.to_datetime)})

        order_matrix = (each_df
                        .loc[:, SVD_KEYS]
                        .pivot(index=ATTR_KEY, columns=TIME_KEY, values=ORD_AMT_KEY)
                        .sort_index()
                        .fillna(value=0))

        # save split
        key1, key2 = eval('df_divide_key.' + PGM), eval('df_divide_key.' + PRD)
        split_df[key1, key2] = order_matrix

    return split_df


def make_na_when_sold_out(view_part, order_, is_catv):
    order_attr_cd = order_.index
    supply_attr_cd = view_part[SUPPLY_AMT_KEY].loc[lambda x: x != 0].index

    if supply_attr_cd.empty:
        return order_

    elif order_attr_cd.difference(supply_attr_cd).empty:
        return find_sold_out(order_, view_part, is_catv)

    else:
        supply_missing_order = order_.loc[order_.index.difference(supply_attr_cd)]
        supply_given_order = order_.loc[order_.index.intersection(supply_attr_cd)]
        supply_given_order_ = find_sold_out(supply_given_order, view_part, is_catv)
        bind_both = pd.concat([supply_missing_order, supply_given_order_], axis=0).sort_index()
        return bind_both


def add_estim_qty(view_part, svd_imputed):
    added = svd_imputed.sum(axis=1).to_frame(name='ESTIM_QTY')
    view_part.update(added)
    return view_part


def prepare_columns(view):
    add_this_col = [i for i in COLUMNS if i not in view.columns.tolist()]
    return pd.concat([view, pd.DataFrame(columns=add_this_col)], sort=False)


def find_sold_out(order_, view_part, is_catv):
    """
    방송중 누적판매량이 일시품절 기준에 적합한지 체크하고
    일시품절로 판단되면 null 값 처리 합니다.
    :param order_:
    :param view_part:
    :param is_catv:
    :return:
    """

    order__ = order_.copy()
    supply = view_part[SUPPLY_AMT_KEY].loc[lambda x: x != 0]
    cum_order = order__.cumsum(axis=1).ffill(axis=1).fillna(value=0)
    total_sales = order__.sum(axis=1)
    buffer = supply.multiply(.9).map(pd.np.ceil)

    attributes = total_sales.index
    for attribute in attributes:
        attr_total_sales, attr_buffer, attr_supply = total_sales[attribute], buffer[attribute], supply[attribute]
        if attr_total_sales >= attr_supply:  # order >= supply인 순간부터 일시품절로 취급
            order_before_sold_out = cum_order.loc[attribute, :].loc[lambda x: x <= attr_supply]
            if order_before_sold_out.empty:
                sold_out_time = cum_order.loc[attribute, :].idxmin()
            else:
                sold_out_time = order_before_sold_out.idxmax()
            order__.loc[attribute, sold_out_time:] = pd.np.nan

        elif is_catv and attr_total_sales > attr_buffer:  # order > 0.9 supply인 경우 일시품절 구간
            """
            :주의:
            buffer만 고려하기에 실제 판매량보다 일찍 품절을 걸게 되고
            추정 판매량이 실제 판매량을 하회할 수 있음. 
            """

            in_buffer_order = (cum_order
                               .loc[attribute]
                               .loc[lambda x: x >= attr_buffer]
                               .diff()
                               .dropna())

            slope = (in_buffer_order
                     .rolling(window=BUFFER_TIME_SIZE)
                     .sum()
                     .shift(-1 * BUFFER_TIME_SIZE + 1)
                     .dropna())

            slope_zero_interval = slope.loc[lambda x: x == 0]
            if slope_zero_interval.empty:
                continue
            else:
                sold_out_time = slope_zero_interval.idxmin()
                order__.loc[attribute, sold_out_time:] = pd.np.nan
        else:  # no sold out, return itself no na
            pass

    return order__


def deprecated_drop_order_zero_assort(order_for_svd):
    """
    deprecated
    :param order_for_svd:
    :return:
    """
    return order_for_svd[~(order_for_svd == 0).all(axis=1)]


def svd_impute(order_with_na):
    """
    order_for_svd_drop: 분당 주문 데이터 - 일시품절 구간 na
    train_set: 아소트 정상 판매된 분당 판매량 - svd 학습
    test_set: 일시 품절 발생한 train set 결측치 - svd 예측
    """

    random.seed(SEED)
    np.random.seed(SEED)

    """split all zero attributes"""
    all_zero_attributes = (order_with_na == 0).all(axis=1).loc[lambda x: x == True].index
    order_with_na_ = order_with_na.drop(all_zero_attributes)

    """svd require long shape"""
    sold_per_minute_long_reshape = (order_with_na_
                                    .reset_index()
                                    .melt(id_vars=ATTR_KEY,
                                          value_name=ORD_AMT_KEY,
                                          var_name=TIME_KEY))

    """check if acceptable"""
    predictable = sold_per_minute_long_reshape[ORD_AMT_KEY].isna().sum() >= 1
    if not predictable:  # return order itself  if no null
        return order_with_na

    train_set = sold_per_minute_long_reshape.dropna().copy()
    long_enough = train_set.shape[0] >= 5
    if not long_enough:
        return order_with_na

    """train svd"""
    sold_out_filter = sold_per_minute_long_reshape[ORD_AMT_KEY].isna()
    test_set = sold_per_minute_long_reshape[sold_out_filter].copy()
    # convert to surprise data type
    max_value = train_set[ORD_AMT_KEY].max()
    train_set_svd_object = Dataset.load_from_df(train_set, Reader(rating_scale=(0, max_value)))
    # find appropriate parameter grid search
    num_of_factor_candidates = 5
    num_of_attr = train_set[ATTR_KEY].unique().shape[0]
    n_factor_min = (num_of_attr // num_of_factor_candidates) + 1
    n_factor_max = num_of_attr - 1
    factor_candidates = np.linspace(start=n_factor_min, stop=n_factor_max, num=num_of_factor_candidates)
    n_factors_grid_search_pool = np.unique(np.floor(factor_candidates)).astype(int)  # svd의 파라미터로 쓸 latent factor 개수

    grid_search_pool = {'reg_all': [0],
                        'lr_all': [0.003, 0.001],
                        'n_factors': n_factors_grid_search_pool,
                        'n_epochs': [15, 22, 30],
                        'biased': [False]}
    error_measure = 'mae'
    grid_searcher = GridSearchCV(SVD, grid_search_pool, measures=[error_measure], cv=5,
                                 n_jobs=N_CORE)  # n_jobs: parallel compute

    try:
        grid_searcher.fit(train_set_svd_object)
    except:
        grid_searcher = GridSearchCV(SVD, grid_search_pool, measures=[error_measure], cv=2,
                                     n_jobs=N_CORE)  # n_jobs: parallel compute
        grid_searcher.fit(train_set_svd_object)

    best_error = grid_searcher.best_score[error_measure]
    best_param = grid_searcher.best_params[error_measure]
    """svd predict"""
    svd = SVD(**best_param).fit(train_set_svd_object.build_full_trainset())

    def _predict(test_set):
        return svd.predict(test_set[ATTR_KEY], test_set[TIME_KEY]).est

    for predict_row_idx, each_blank in test_set.iterrows():
        predicted_value = _predict(each_blank)
        test_set.loc[predict_row_idx, ORD_AMT_KEY] = predicted_value

    filled_sold_per_minute_long_shape = pd.concat([train_set, test_set]).round(0)
    imputed = filled_sold_per_minute_long_shape.pivot(index=ATTR_KEY, columns=TIME_KEY, values=ORD_AMT_KEY)

    """merge all zeros"""
    for attribute in all_zero_attributes:
        imputed.loc[attribute, :] = 0

    return imputed


def add_sold_out_dtm(view_part, order_with_na):
    dtms_when_soldout = order_with_na.apply(lambda x: x.loc[x.isna()].index.min(), 1)
    soldout_dtm = dtms_when_soldout.to_frame(name='SOLD_OUT_DTM')
    soldout_dtm['SOLD_OUT_FLAG'] = pd.np.select([soldout_dtm.isna(), soldout_dtm.notna()], [0, 1])
    return view_part.update(soldout_dtm)


def set_index(view):
    return view.set_index(ATTR_KEY)


def add_no_ordered_attr(order, view_part):
    """
    주문이 0인 속성코드는 svd 결과에 포함되지 않음
    그 때문에 집계 view df와 조인시 서로 맞지 않음.
    사후적으로 0 값을 명시적으로 집어넣는 편이 이후 작업을 간편하게 해줄 것
    pseudo code
    view 데이터의 속성코드를 가져와서
    order 행에 0 값으로 채운다
    :return:
    """

    order_with_na_ = order.copy()

    attributes_in_order = order_with_na_.index.values
    attributes_in_view = view_part.index.values

    not_in_order = ~ pd.np.isin(attributes_in_view, attributes_in_order)
    add_this_attributes = attributes_in_view[not_in_order]
    for add in add_this_attributes:
        order_with_na_.loc[add, :] = 0

    return order_with_na_


def _weekday_en_to_kr(view_part):
    tmp = view_part.PGM_TIME
    for i, j in WEEKDAY_EN_TO_KR.items():
        tmp = tmp.str.replace(i, j)
    view_part.PGM_TIME = tmp
    return view_part


def add_pgm_time_string(view_part):
    added = (view_part
             .BROAD_STR_DTM.dt
             .strftime('%m/%d (%a) %H시')
             .to_frame(name='PGM_TIME'))
    view_part.update(added)
    view_part = _weekday_en_to_kr(view_part)
    return view_part


def fill_blank_order(view_part: pd.DataFrame):
    orders = ['TOT_ORD_QTY', 'PORD_03_TOT_ORD_QTY']
    view_part[orders] = view_part[orders].fillna(value=0)
    return view_part


def add_attribute_detail(view_part: pd.DataFrame):
    attributes = view_part.ATTR_PRD_WHL_VAL.str.split(',', expand=True)
    if attributes.shape[1] != 4:
        paste_leftover = attributes.iloc[:, 3:].apply(lambda x: ' '.join(x[~pd.isnull(x)].values), axis=1)
        attributes.loc[:, 3] = paste_leftover
        attributes = attributes.loc[:, :3]
        assert attributes.shape[1] == 4, 'attribute shape over 4'
    attributes = attributes.loc[:, [2, 3]].rename(columns={2: 'SIZE2', 3: 'SIZE3'})
    assert attributes.shape[1] == 2
    view_part.update(attributes)
    return view_part


def column_translate(view_part: pd.DataFrame):
    if view_part.index.name in COLUMNS:
        view_part = view_part.reset_index(drop=False).copy()
    assert view_part.shape[1] == COL_NAME_EN_TO_KR.__len__(), 'column length not match with translator'
    view_part = view_part.rename(columns=COL_NAME_EN_TO_KR)
    return view_part


def column_inv_translate(view):
    return view.rename(columns={j: i for i, j in COL_NAME_EN_TO_KR.items()})

def get_snapshot_each_sold_out(view_part, order_with_na):
    """
    order_with_na에서 na가 발생한 시점만 누적주문량을 딴다.

    sold_out_dtm의 누적주문을 잡고
    그 시점이 몇번째 인지 카운트
    :return:
    """
    cumulative_order = order_with_na.fillna(value=0).cumsum(axis=1)
    sold_out_dtm = view_part['SOLD_OUT_DTM']
    tmp_soldout_dtm = sold_out_dtm.fillna(value=cumulative_order.columns.max())
    unique_sold_out_dtms = pd.np.sort(tmp_soldout_dtm.unique())

    stack = pd.DataFrame()
    for seq, dtm in enumerate(unique_sold_out_dtms):
        seq += 1
        sold_out_snapshot = pd.DataFrame()
        sold_out_snapshot['ATTR_PRD_CD'] = cumulative_order.index
        sold_out_snapshot['SOLDOUT_SEQ'] = seq
        sold_out_snapshot['VALUE'] = cumulative_order.loc[:, dtm].values
        stack = stack.append(sold_out_snapshot)

    return stack


def add_first_sold_out_snapshot(view_part, sold_out_snapshot):
    first_sold_out_snapshot = (sold_out_snapshot
                               .query('SOLDOUT_SEQ == 1')
                               .set_index('ATTR_PRD_CD')
                               .drop(['SOLDOUT_SEQ'], axis=1))
    return view_part.update(first_sold_out_snapshot)


def astype_view(view):
    return view.astype(VIEW_DTYPES)


def astype_svd(svd):
    return svd.astype(SVD_DTYPES)
