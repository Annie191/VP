-- 单表查询: 结果 1,2,3,4 (NULL最前)
SELECT vp_rowid FROM source WHERE '2018-04-13' ORDER BY (-c0) DESC, vp_rowid;