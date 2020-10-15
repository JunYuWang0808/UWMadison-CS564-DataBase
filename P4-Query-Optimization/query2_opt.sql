.timer on

SELECT
	s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM
	part, supplier, partsupp,
     (SELECT N_NATIONKEY, N_NAME
    FROM NATION
    WHERE EXISTS(SELECT * FROM REGION WHERE R_REGIONKEY = N_REGIONKEY and r_name = 'ASIA')) as o
WHERE
	p_partkey = ps_partkey
	AND s_suppkey = ps_suppkey
	AND p_size = 11 -- [SIZE]
	AND p_type like 'MEDIUM BRUSHED COPPER' -- '%[TYPE]'
	AND s_nationkey = o.n_nationkey
	AND ps_supplycost = (
		SELECT
			min(ps_supplycost)
		FROM
			partsupp, supplier,  (SELECT N_NATIONKEY
    FROM NATION
    WHERE EXISTS(SELECT * FROM REGION WHERE R_REGIONKEY = N_REGIONKEY and r_name = 'ASIA')) as o
		WHERE
			p_partkey = ps_partkey
			AND s_suppkey = ps_suppkey
			AND s_nationkey = o.n_nationkey
		)
ORDER BY
	s_acctbal DESC,
	n_name,
	s_name,
	p_partkey;
