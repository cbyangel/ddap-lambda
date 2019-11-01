WITH PGM AS
         (
             SELECT B.PGM_ID
                  , B.PRD_CD
                  , A.CHANL_CD
                  , A.BROAD_DT
             FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
                      INNER JOIN GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
                                 ON A.PGM_ID = B.PGM_ID
                      INNER JOIN GSBI_OWN.D_PRD_PRD_M P
                                 on B.PRD_CD = P.PRD_CD
             WHERE A.PGM_ID = B.PGM_ID
               AND A.BROAD_DT = :broad_dt
               AND A.CHANL_CD = :chanl_cd       -- 'C' --'H'
         )

SELECT /*+ FULL(ORD) PARALLEL(ORD, 4) */
    ORD.PGM_ID
     , max(ORD.PRD_CD)  as PRD_CD
     , TMP.CHANL_CD
     , ORD.ATTR_PRD_CD
     , ORD.ORD_DT
     , ORD.ORD_TIME
     , SUM(TOT_ORD_QTY) AS TOT_ORD_QTY
FROM GSBI_OWN.F_ORD_ORD_D ORD
         INNER JOIN PGM TMP
                    ON ORD.PRD_CD = TMP.PRD_CD
                        AND ORD.PGM_ID = TMP.PGM_ID
WHERE ORD.ORD_DT >= TMP.BROAD_DT
  AND ORD.ORD_DT <= TMP.BROAD_DT + 1
  AND ORD.BROAD_ORD_TIME_DTL_CD IN ('O1')
  and ORD.TOT_ORD_CUST_NO > 0
GROUP BY ORD.PGM_ID
       , TMP.CHANL_CD
       , ORD.ATTR_PRD_CD
       , ORD.ORD_DT
       , ORD.ORD_TIME
