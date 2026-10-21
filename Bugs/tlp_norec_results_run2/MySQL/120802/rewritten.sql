DROP DATABASE IF EXISTS codex_db7;
CREATE DATABASE codex_db7;
USE codex_db7;
SET SESSION sql_mode =
'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION optimizer_switch =
'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,hash_set_operations=on';
SET SESSION big_tables = 1;
SET SESSION internal_tmp_mem_storage_engine = 'MEMORY';
CREATE TABLE s(
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 FLOAT NULL,
  c1 BIGINT NULL
) ENGINE=InnoDB;
INSERT INTO s(c0, c1) VALUES (NULL, 0);
CREATE TABLE l(
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c1 BIGINT NULL,
  KEY l_idx(c1)
) ENGINE=InnoDB;
CREATE TABLE r(
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 FLOAT NULL
) ENGINE=InnoDB;
INSERT INTO l(vp_rowid, c1)
SELECT vp_rowid, c1 FROM s;
INSERT INTO r(vp_rowid, c0)
SELECT vp_rowid, c0 FROM s;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT DISTINCTROW c0 AS ref0
FROM s
WHERE c1 IN (COALESCE(0.36047097332362965, NULL, NULL, 275915752));

-- SKIPPED TLP/NoREC: DISTINCT is not handled by this batch rewriter
