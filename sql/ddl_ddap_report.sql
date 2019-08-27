-- auto-generated definition
create table DDAP_REPORT_AUTO
(
    아이템명       VARCHAR2(200),
    아이템코드      NUMBER(14),
    사이즈_체계     VARCHAR2(200),
    컬러수        NUMBER(2),
    상품명        VARCHAR2(500),
    상품코드       NUMBER(14),
    런칭년도       NUMBER(4),
    런칭월        NUMBER(2),
    속성코드       NUMBER(14)    not null,
    방송채널코드     VARCHAR2(5),
    방송횟차       NUMBER(3),
    방송코드       NUMBER(14)    not null,
    컬러         VARCHAR2(200),
    사이즈        VARCHAR2(200),
    사이즈_변환     VARCHAR2(200),
    사이즈2       varchar2(200),
    사이즈3       varchar2(200),
    품절여부       NUMBER(1),
    일시품절       TIMESTAMP(6) default NULL,
    공급약속수량     NUMBER(5),
    미리주문       NUMBER(5),
    첫일시품절시_수량  NUMBER(5),
    추정_기대수요_수량 NUMBER(5),
    실제_총주문_수량  NUMBER(5),
    프로그램명      VARCHAR2(100),
    방송일자       VARCHAR2(100) not null,
    방송일자시간     TIMESTAMP(6),
    방송시간       VARCHAR2(200),
    노출분        NUMBER(5),
    데이터런칭일자    NUMBER(10),
    총주문_천원     NUMBER(10),
    순주문_천원     NUMBER(10),
    전환율        NUMBER(20, 10),
    가중분        NUMBER(20, 10),
    가중분취       NUMBER(20, 10),
    상품속성값      VARCHAR2(200)
)
/

comment on table DDAP_REPORT_AUTO
    is 'DDAP 자동화 테이블'
/
CREATE UNIQUE INDEX DTA_OWN.PK_DDAP_REPORT_AUTO ON DDAP_REPORT_AUTO (방송코드, 속성코드)
/
ALTER TABLE DDAP_REPORT_AUTO
    ADD CONSTRAINT PK_DDAP_REPORT_AUTO PRIMARY KEY (방송코드, 속성코드);
/
GRANT SELECT, UPDATE, DELETE, INSERT ON DDAP_REPORT_AUTO TO RL_DTA_DML;
GRANT SELECT ON DDAP_REPORT_AUTO TO RL_DTA_SEL;
/
CREATE OR REPLACE PUBLIC SYNONYM DDAP_REPORT_AUTO FOR DDAP_REPORT_AUTO
/
