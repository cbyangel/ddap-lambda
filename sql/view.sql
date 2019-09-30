WITH TMP_PGM AS (
    select B.PGM_ID
         , B.PRD_CD
         , B.CHANL_CD
         , A.BROAD_STR_DTM
         , A.BROAD_DT
         , A.PGM_NM
         , A.MNFC_GBN_CD
         , C.BROAD_SEQ
    FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
             INNER JOIN GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
                        ON A.PGM_ID = B.PGM_ID
             INNER JOIN (
        SELECT X1.PRD_CD,
               X1.CHANL_CD,
               X1.PGM_ID,
               dense_rank() OVER (PARTITION BY X1.PRD_CD, X1.CHANL_CD ORDER BY X2.BROAD_STR_DTM) AS BROAD_SEQ
        FROM GSBI_OWN.D_BRD_BROAD_FORM_PRD_M X1
                 INNER JOIN GSBI_OWN.D_BRD_BROAD_FORM_M X2 ON X1.PGM_ID = X2.PGM_ID
    ) C
                        ON B.PGM_ID = C.PGM_ID
                            AND B.PRD_CD = C.PRD_CD
             inner join GSBI_OWN.D_PRD_PRD_M P
                        on B.PRD_CD = P.PRD_CD
    WHERE A.BROAD_DT = :broad_dt
      and A.CHANL_CD = :chanl_cd       --'H'
      and A.MNFC_GBN_CD = :mnfc_gbn_cd -- '5'
)
   , TMP_PRD AS (
    SELECT A.PRD_CD
         , A.MD_ID
         , A.PRD_NM
         , I.ITEM_NM
         , I.ITEM_CD
         , B.ATTR_PRD_CD
         , A.FST_BROAD_DT
         , SUBSTR(A.FST_BROAD_DT, 1, 4) AS FST_BROAD_YYYY
         , SUBSTR(A.FST_BROAD_DT, 5, 2) AS FST_BROAD_MM
         , B.ATTR_PRD_REP_CD
         , B.ATTR_PRD_WHL_VAL
         , B.ATTR_PRD_1_VAL
         , CASE
               WHEN INSTR(B.ATTR_PRD_2_VAL, '(') > 0
                   THEN SUBSTR(B.ATTR_PRD_2_VAL, 1, INSTR(B.ATTR_PRD_2_VAL, '(') - 1)
               ELSE B.ATTR_PRD_2_VAL END   ATTR_PRD_2_VAL
         , B.ATTR_PRD_2_VAL             AS ATTR_PRD_2_VAL_ORG
    FROM GSBI_OWN.D_PRD_PRD_M A
             INNER JOIN GSBI_OWN.D_PRD_ATTR_PRD_M B
                        ON A.PRD_CD = B.PRD_CD
             inner join GSBI_OWN.D_PRD_ITEM_M I
                        on A.ITEM_CD = I.ITEM_CD
             INNER JOIN (SELECT DISTINCT PRD_CD FROM TMP_PGM) C
                        ON A.PRD_CD = C.PRD_CD
)
   , TMP_PRD_ATTR_INFO AS (
    SELECT PRD_CD
         , SUM(COLOR_CNT) AS COLOR_CNT
         , MAX(SIZE_STR)  AS SIZE_STR
    FROM (
             SELECT PRD_CD
                  , COUNT(DISTINCT ATTR_PRD_1_VAL) AS COLOR_CNT
                  , ''                             AS SIZE_STR
             FROM TMP_PRD
             GROUP BY PRD_CD
             UNION ALL
             SELECT PRD_CD
                  , 0 AS COLOR_CNT
                  , LISTAGG(ATTR_PRD_2_VAL, '_') WITHIN GROUP (ORDER BY CASE
                                                                            WHEN ATTR_PRD_2_VAL = 'SS' THEN '10'
                                                                            WHEN ATTR_PRD_2_VAL = 'S' THEN '20'
                                                                            WHEN ATTR_PRD_2_VAL = 'M' THEN '30'
                                                                            WHEN ATTR_PRD_2_VAL = 'L' THEN '40'
                                                                            WHEN ATTR_PRD_2_VAL = 'XL' THEN '50'
                                                                            WHEN ATTR_PRD_2_VAL = '2XL' THEN '60'
                                                                            WHEN ATTR_PRD_2_VAL = 'XXL' THEN '60'
                 /* 숫자가 있는 경우 숫자만 추출하여 순서 정렬 (5자리로 맞춰서 정렬) */
                                                                            ELSE CASE
                                                                                     WHEN LENGTH(REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')) = 1
                                                                                         THEN '1000' || REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')
                                                                                     WHEN LENGTH(REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')) = 2
                                                                                         THEN '100' || REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')
                                                                                     WHEN LENGTH(REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')) = 3
                                                                                         THEN '10' || REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')
                                                                                     ELSE '1' || REGEXP_SUBSTR(ATTR_PRD_2_VAL, '[0-9]+')
                                                                                END
                 END) AS SIZE_STR
             FROM (
                      SELECT DISTINCT PRD_CD, ATTR_PRD_2_VAL
                      FROM TMP_PRD
                  )
             GROUP BY PRD_CD
         ) A
    GROUP BY PRD_CD
)
   , TMP_MINUTE AS (SELECT PGM_ID
                         , PRD_CD
                         , SUM(WEIHT_MI) AS WEIHT_MI
                         , sum(EXPOS_MI) as expos_mi
                    FROM GSBI_OWN.F_ORD_BROAD_TGT_D
                    GROUP BY PGM_ID
                           , PRD_CD
)
   , TMP_SPLY AS (SELECT
                      /*+ FULL(MNG) FULL(APNT) FULL(PGM) */
                      MNG.PGM_ID
                       , MNG.PRD_CD
                       , APNT.ATTR_PRD_REP_CD
                       , SUM(APNT.SUPLY_APNT_QTY) AS SUPLY_APNT_QTY
                       , SUM(MNG.SUPLY_PSBL_QTY)  AS SUPLY_PSBL_QTY
                       , SUM(MNG.PGM_TGT_QTY)     AS PGM_TGT_QTY
                  FROM SDHUB_OWN.STG_ATP_BROAD_BEF_MNG_M MNG
                           INNER JOIN SDHUB_OWN.STG_ATP_SUPLY_APNT_D APNT
                                      ON MNG.SUPLY_PLAN_MNG_NO = APNT.SUPLY_PLAN_MNG_NO
                                          AND MNG.PRD_CD = APNT.PRD_CD
                           INNER JOIN TMP_PGM PGM
                                      ON MNG.PGM_ID = PGM.PGM_ID
                                          AND MNG.PRD_CD = PGM.PRD_CD
                  WHERE MNG.BROAD_BEF_MNG_PRG_ST_CD <> '0'
                  GROUP BY MNG.PGM_ID
                         , MNG.PRD_CD
                         , APNT.ATTR_PRD_REP_CD
)
   , TMP_ORD_BASE AS (
    SELECT /*+ FULL(ORD) FULL(TMP_PGM) PARALLEL(ORD, 4) PARALLEL(TMP_PGM, 4)*/
        ORD.PGM_ID
         , ORD.PRD_CD
         , ORD.ATTR_PRD_CD
         , ORD.TOT_ORD_QTY
         , ORD.NET_ORD_AMT
         , ORD.SUP_SHR_NET_ORD_DC_AMT
         , ORD.NET_ORD_QTY
         , ORD.NET_ORD_NACCM_SALE_AMT
         , ORD.TOT_ORD_AMT
         , ORD.SUP_SHR_TOT_ORD_DC_AMT
         , ORD.CMM_CPN_DC_AMT
         , ORD.NACCM_SALE_SUBTR_APPLY_RT
         , ORD.TOT_ORD_NACCM_SALE_AMT
         , ORD.CHG_RT
         , ORD.BROAD_RLRSLT_GBN_CD
         , ORD.ORD_DT
         , ORD.ORD_TIME
         , ORD.BROAD_ORD_TIME_DTL_CD
    FROM GSBI_OWN.F_ORD_ORD_D ORD
             INNER JOIN TMP_PGM
                        ON ORD.PRD_CD = TMP_PGM.PRD_CD
                            AND ORD.PGM_ID = TMP_PGM.PGM_ID
                            and ORD.BROAD_RLRSLT_GBN_CD = TMP_PGM.CHANL_CD
    WHERE ORD.ORD_DT >= TMP_PGM.BROAD_DT
      and ORD.ORD_DT <= TMP_PGM.BROAD_DT + 1
      AND ORD.BROAD_ORD_TIME_DTL_CD IN ('P1', 'O1')
      AND ORD.TOT_ORD_CUST_NO > 0
)
   , TMP_ORD1 AS (
    SELECT ORD.PGM_ID
         , MAX(TMP_PGM.CHANL_CD)
         , ORD.PRD_CD                                                                                  as PRD_CD
         , MAX(ORD.CHG_RT)                                                                             AS CHG_RT
         , SUM(ORD.TOT_ORD_AMT + ORD.SUP_SHR_TOT_ORD_DC_AMT + ORD.CMM_CPN_DC_AMT * ORD.TOT_ORD_QTY +
               ORD.TOT_ORD_NACCM_SALE_AMT)                                                             AS TOT_ORD_AMT
         , SUM(ORD.NET_ORD_AMT + ORD.SUP_SHR_NET_ORD_DC_AMT + ORD.CMM_CPN_DC_AMT * ORD.NET_ORD_QTY +
               ORD.NET_ORD_NACCM_SALE_AMT)                                                             AS NET_ORD_AMT
         , SUM(ROUND((ORD.TOT_ORD_AMT + ORD.SUP_SHR_TOT_ORD_DC_AMT + ORD.CMM_CPN_DC_AMT * ORD.TOT_ORD_QTY +
                      ORD.TOT_ORD_NACCM_SALE_AMT) * ORD.CHG_RT * (1 - ORD.NACCM_SALE_SUBTR_APPLY_RT))) AS EXPCT_SAL_AMT
    FROM TMP_ORD_BASE ORD
             INNER JOIN TMP_PGM
                        ON ORD.PRD_CD = TMP_PGM.PRD_CD
                            AND ORD.PGM_ID = TMP_PGM.PGM_ID
                            and ORD.BROAD_RLRSLT_GBN_CD = TMP_PGM.CHANL_CD
    WHERE ORD.BROAD_ORD_TIME_DTL_CD IN ('P1', 'O1')
    GROUP BY ORD.PGM_ID
           , ORD.PRD_CD
)
   , TMP_ORD2 AS (
    SELECT ORD.PGM_ID
         , MAX(TMP_PGM.CHANL_CD) AS CHANL_CD
         , MAX(ORD.PRD_CD)       AS PRD_CD
         , ATTR_PRD_CD
         , SUM(CASE
                   WHEN ORD.ORD_DT || ORD.ORD_TIME BETWEEN TO_CHAR(TRUNC(TMP_PGM.BROAD_STR_DTM) + 3 / 24, 'YYYYMMDDHH24MI') AND TO_CHAR(TMP_PGM.BROAD_STR_DTM, 'YYYYMMDDHH24MI')
                       THEN TOT_ORD_QTY
        END)                     AS PORD_03_TOT_ORD_QTY
    FROM TMP_ORD_BASE ORD
             INNER JOIN TMP_PGM
                        ON ORD.PRD_CD = TMP_PGM.PRD_CD
                            AND ORD.PGM_ID = TMP_PGM.PGM_ID
                            and ORD.BROAD_RLRSLT_GBN_CD = TMP_PGM.CHANL_CD
    GROUP BY ORD.PGM_ID
           , ORD.ATTR_PRD_CD
)
   , TMP_ORD3 AS (
    SELECT ORD.PGM_ID
         , MAX(TMP_PGM.CHANL_CD) AS CHANL_CD
         , ATTR_PRD_CD
         , SUM(ORD.TOT_ORD_QTY)  AS TOT_ORD_QTY
    FROM TMP_ORD_BASE ORD
             INNER JOIN TMP_PGM
                        ON ORD.PRD_CD = TMP_PGM.PRD_CD
                            AND ORD.PGM_ID = TMP_PGM.PGM_ID
                            and ORD.BROAD_RLRSLT_GBN_CD = TMP_PGM.CHANL_CD
    WHERE ORD.BROAD_ORD_TIME_DTL_CD IN ('O1')
    GROUP BY ORD.PGM_ID
           , ORD.ATTR_PRD_CD
)


SELECT /*+ PARALLEL(4) */
    TMP_PRD.ITEM_NM
     , TMP_PRD.ITEM_CD
     , TMP_PRD.PRD_NM
     , TMP_PRD.PRD_CD
     , TMP_PRD_ATTR_INFO.SIZE_STR
     , TMP_PRD_ATTR_INFO.COLOR_CNT
     , TMP_PRD.FST_BROAD_YYYY
     , TMP_PRD.FST_BROAD_MM
     , TMP_PRD.ATTR_PRD_CD
     , TMP_PGM.CHANL_CD
     , TMP_PGM.BROAD_SEQ
     , TMP_PRD.ATTR_PRD_1_VAL
     , TMP_PRD.ATTR_PRD_2_VAL_ORG
     , TMP_PRD.ATTR_PRD_2_VAL
     , TMP_PRD.ATTR_PRD_WHL_VAL
     , NVL(TMP_SPLY.SUPLY_APNT_QTY, 0)    AS SUPLY_APNT_QTY
     , TMP_ORD3.TOT_ORD_QTY
     , TMP_ORD2.PORD_03_TOT_ORD_QTY
     , CEIL(TMP_ORD1.TOT_ORD_AMT / 1000)  as TOT_ORD_AMT
     , CEIL(TMP_ORD1.NET_ORD_AMT / 1000)  as NET_ORD_AMT
     , TMP_ORD1.CHG_RT
     , TMP_PGM.PGM_ID
     , TMP_PGM.PGM_NM
     , TMP_PGM.BROAD_DT
     , TMP_PGM.BROAD_STR_DTM
     , CASE
           WHEN TMP_MINUTE.WEIHT_MI > 0 THEN ROUND(TMP_ORD1.EXPCT_SAL_AMT / (TMP_MINUTE.WEIHT_MI / 60))
           ELSE 0 END                        EXPCT_SAL_AMT_PER_WMI
     , ROUND(TMP_MINUTE.WEIHT_MI / 60)    AS WEIHT_MI
     , ROUND(TMP_MINUTE.EXPOS_MI / 60)    AS EXPOS_MI
     , (SELECT MIN(BROAD_DT)
        FROM GSBI_OWN.D_BRD_BROAD_FORM_PRD_M X1
                 INNER JOIN GSBI_OWN.D_BRD_BROAD_FORM_M X2 ON X1.PGM_ID = X2.PGM_ID
        WHERE X1.PRD_CD = TMP_PRD.PRD_CD) AS MYSHOP_FST_BROAD_DT
     , TMP_PRD.MD_ID
FROM TMP_PRD
         INNER JOIN TMP_PRD_ATTR_INFO
                    ON TMP_PRD.PRD_CD = TMP_PRD_ATTR_INFO.PRD_CD
         INNER JOIN TMP_PGM
                    ON TMP_PRD.PRD_CD = TMP_PGM.PRD_CD
         left outer join TMP_MINUTE
                         ON TMP_PGM.PRD_CD = TMP_MINUTE.PRD_CD
                             AND TMP_PGM.PGM_ID = TMP_MINUTE.PGM_ID
         LEFT OUTER JOIN TMP_SPLY
                         ON TMP_PGM.PRD_CD = TMP_SPLY.PRD_CD
                             AND TMP_PGM.PGM_ID = TMP_SPLY.PGM_ID
                             AND TMP_PRD.ATTR_PRD_REP_CD = TMP_SPLY.ATTR_PRD_REP_CD
         LEFT OUTER JOIN TMP_ORD1
                         ON TMP_PGM.PRD_CD = TMP_ORD1.PRD_CD
                             AND TMP_PGM.PGM_ID = TMP_ORD1.PGM_ID
         LEFT OUTER JOIN TMP_ORD2
                         ON TMP_PRD.PRD_CD = TMP_ORD2.PRD_CD
                             and TMP_PRD.ATTR_PRD_CD = TMP_ORD2.ATTR_PRD_CD
                             and TMP_PGM.PGM_ID = TMP_ORD2.PGM_ID
         LEFT OUTER JOIN TMP_ORD3
                         ON TMP_PRD.ATTR_PRD_CD = TMP_ORD3.ATTR_PRD_CD
                             and TMP_PGM.PGM_ID = TMP_ORD3.PGM_ID
order by PGM_ID, ATTR_PRD_CD
