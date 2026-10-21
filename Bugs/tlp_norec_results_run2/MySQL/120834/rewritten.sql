DROP DATABASE IF EXISTS repro628_5;
CREATE DATABASE repro628_5;
USE repro628_5;
SET SESSION sql_mode =
  'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION optimizer_switch =
  'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,hash_set_operations=on';
CREATE TABLE s (
  id BIGINT NOT NULL PRIMARY KEY,
  x DOUBLE NULL
) ENGINE=InnoDB;
INSERT INTO s VALUES
  (1, NULL),
  (2, 1.0E308),
  (3, 1.0E308);
CREATE INDEX sx ON s(x, id);
CREATE TABLE l (
  id BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
CREATE TABLE r (
  id BIGINT NOT NULL PRIMARY KEY,
  x DOUBLE NULL
) ENGINE=InnoDB;
INSERT INTO l SELECT id FROM s;
INSERT INTO r SELECT id, x FROM s;
CREATE VIEW v AS
SELECT l.id, r.x
FROM l JOIN r ON l.id = r.id;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT DISTINCT
  SUM(DISTINCT 0.829074650150795) AS a,
  ((COALESCE(s.x, s.id)) NOT IN (CAST(s.id AS SIGNED)))
    || ((+ ((s.x) >= (s.id)))) AS b,
  EXISTS (SELECT 1 WHERE FALSE) AS c,
  AVG(x) AS d
FROM s
GROUP BY
  ((COALESCE(s.x, s.id)) NOT IN (CAST(s.id AS SIGNED)))
    || ((+ ((s.x) >= (s.id))));

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
