DROP DATABASE IF EXISTS repro73_42;
CREATE DATABASE repro73_42;
USE repro73_42;

SET SESSION optimizer_switch =
  'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,hash_set_operations=on';

CREATE TABLE s (
  id BIGINT NOT NULL PRIMARY KEY,
  c0 FLOAT,
  c1 DECIMAL(20,0)
) ENGINE=InnoDB;

INSERT INTO s VALUES (1, 0, 954517769);

CREATE INDEX s_c0 ON s(c0);

CREATE TABLE l (
  id BIGINT NOT NULL PRIMARY KEY,
  c0 FLOAT
) ENGINE=InnoDB;

CREATE TABLE r (
  id BIGINT NOT NULL PRIMARY KEY,
  c1 DECIMAL(20,0),
  c0 FLOAT
) ENGINE=InnoDB;

INSERT INTO l SELECT id, c0 FROM s;
INSERT INTO r SELECT id, c1, c0 FROM s;

CREATE VIEW v AS
SELECT l.id, l.c0, r.c1
FROM l JOIN r ON l.id = r.id;

SELECT 'single' AS qt, s.c1 AS ref0
FROM s
WHERE s.c0 < IFNULL(1.7976931348623157E308, -1605651052)
GROUP BY s.c1;

SELECT 'split' AS qt, v.c1 AS ref0
FROM v
WHERE v.c0 < IFNULL(1.7976931348623157E308, -1605651052)
GROUP BY v.c1;