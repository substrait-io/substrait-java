CREATE TABLE IF NOT EXISTS nation
(
    n_nationkey integer,
    n_name      char(25),
    n_regionkey integer,
    n_comment   varchar(152)
) USING iceberg;
CREATE TABLE IF NOT EXISTS region
(
    r_regionkey integer,
    r_name      char(25),
    r_comment   varchar(152)
) USING iceberg;
CREATE TABLE IF NOT EXISTS part
(
    p_partkey     integer,
    p_name        varchar(55),
    p_mfgr        char(25),
    p_brand       char(10),
    p_type        varchar(25),
    p_size        integer,
    p_container   char(10),
    p_retailprice decimal(15, 2),
    p_comment     varchar(23)
) USING iceberg;
CREATE TABLE IF NOT EXISTS supplier
(
    s_suppkey   integer,
    s_name      char(25),
    s_address   varchar(40),
    s_nationkey integer,
    s_phone     char(15),
    s_acctbal   decimal(15, 2),
    s_comment   varchar(101)
) USING iceberg;
CREATE TABLE IF NOT EXISTS partsupp
(
    ps_partkey    integer,
    ps_suppkey    integer,
    ps_availqty   integer,
    ps_supplycost decimal(15, 2),
    ps_comment    varchar(199)
) USING iceberg;
CREATE TABLE IF NOT EXISTS customer
(
    c_custkey    integer,
    c_name       varchar(25),
    c_address    varchar(40),
    c_nationkey  integer,
    c_phone      char(15),
    c_acctbal    decimal(15, 2),
    c_mktsegment char(10),
    c_comment    varchar(117)
) USING iceberg;
CREATE TABLE IF NOT EXISTS orders
(
    o_orderkey      integer,
    o_custkey       integer,
    o_orderstatus   char(1),
    o_totalprice    decimal(15, 2),
    o_orderdate     date,
    o_orderpriority char(15),
    o_clerk         char(15),
    o_shippriority  integer,
    o_comment       varchar(79)
) USING iceberg;
CREATE TABLE IF NOT EXISTS lineitem
(
    l_orderkey      integer,
    l_partkey       integer,
    l_suppkey       integer,
    l_linenumber    integer,
    l_quantity      decimal(15, 2),
    l_extendedprice decimal(15, 2),
    l_discount      decimal(15, 2),
    l_tax           decimal(15, 2),
    l_returnflag    char(1),
    l_linestatus    char(1),
    l_shipdate      date,
    l_commitdate    date,
    l_receiptdate   date,
    l_shipinstruct  char(25),
    l_shipmode      char(10),
    l_comment       varchar(44)
) USING iceberg;