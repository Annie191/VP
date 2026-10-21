DROP DATABASE IF EXISTS codex_db12;
CREATE DATABASE codex_db12;
USE codex_db12;

SET SESSION sql_mode =
'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

SET SESSION optimizer_switch =
'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,hash_set_operations=on';

SET SESSION big_tables = 0;
SET SESSION internal_tmp_mem_storage_engine = 'MEMORY';

CREATE TABLE s(
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 FLOAT NULL,
  KEY idx(c0 DESC)
) ENGINE=InnoDB;

INSERT INTO s(c0)
VALUES (1903340000), (1894990000), (-0e0), (-0e0), (0e0), (NULL);

CREATE TABLE l(id BIGINT NOT NULL PRIMARY KEY, c0 FLOAT NULL) ENGINE=InnoDB;
CREATE TABLE r(id BIGINT NOT NULL PRIMARY KEY, c0 FLOAT NULL) ENGINE=InnoDB;

INSERT INTO l SELECT id, c0 FROM s;
INSERT INTO r SELECT id, c0 FROM s;

SELECT DISTINCT s.c0 AS ref0
FROM s
GROUP BY s.c0;

SELECT DISTINCT d.c0 AS ref0
FROM (
  SELECT l.id, r.c0
  FROM l
  JOIN r ON l.id = r.id
  WHERE NOT EXISTS (
    SELECT 1
    FROM r rx
    WHERE rx.id = l.id
      AND rx.id <> l.id
  )
) d
GROUP BY d.c0;
