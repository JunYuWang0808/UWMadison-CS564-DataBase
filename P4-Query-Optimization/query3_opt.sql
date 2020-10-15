.timer on
select
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
  (SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY, O_CUSTKEY
  FROM ORDERS
  WHERE o_orderdate < "1995-03-15") as opt_o,
    (SELECT C_CUSTKEY FROM CUSTOMER WHERE c_mktsegment = 'BUILDING') as opt_c,
    (SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM LINEITEM
     WHERE l_shipdate > "1995-03-15") as opt_l
where
  opt_l.l_orderkey = opt_o.o_orderkey
    AND opt_o.O_CUSTKEY = opt_c.C_CUSTKEY
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc,
  o_orderdate;
