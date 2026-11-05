package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;

public class TiDBECSTOracleCandidateSynthesisTest {
    @Test
    void declaresTheImplementedTidbPrototypeCapabilitySet() {
        TiDBEcstCapabilities capabilities = new TiDBEcstCapabilities();

        assertTrue(capabilities.supportsRepresentation(EcstTransformationConfig.Representation.VERTICAL_PARTITIONED));
        assertTrue(capabilities.supportsRepresentation(EcstTransformationConfig.Representation.VIEW));
        assertTrue(!capabilities.supportsRepresentation(EcstTransformationConfig.Representation.REPLICATED));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.JOIN));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.DERIVED_TABLE));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.CTE));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.VIEW));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.WHERE));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.SELECT));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.HAVING));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.JOIN_ON));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.INNER_WHERE));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.INNER_SELECT));
        assertEquals("TiDB ECST prototype capability set", capabilities.describe());
    }

    @Test
    void unsupportedTiFlashTargetsAreExcludedFromDefaultRuntimeTargets() {
        assertTrue(TiDBECSTOracle.defaultTargets().stream().noneMatch(t -> t.getKey().contains("TIFLASH")));
    }

    @Test
    void queryShapeGatesHavingAndSelectContexts() {
        TiDBECSTOracle.QueryShape plain = TiDBECSTOracle.queryShapeFromSql(
                "SELECT c0 FROM t0 WHERE c0 > 0", 1);
        TiDBECSTOracle.QueryShape aggregate = TiDBECSTOracle.queryShapeFromSql(
                "SELECT c0, COUNT(*) FROM t0 GROUP BY c0 HAVING COUNT(*) > 1", 2);

        assertTrue(!plain.hasAggregation());
        assertTrue(plain.hasProjection());
        assertTrue(aggregate.hasAggregation());
        assertTrue(aggregate.hasProjection());
    }

    @Test
    void havingCanUseNonUniqueGroupsAndCount() {
        TiDBVerticalPartitioningOracle oracle = new TiDBVerticalPartitioningOracle(null, false);
        String query = oracle.buildHavingMultiRowProbeForTest("source", "`c0`", List.of("c0"));
        assertTrue(query.contains("GROUP BY `c0`"));
        assertTrue(!query.contains("GROUP BY `vp_rowid`"));
        assertTrue(query.contains("COUNT(*) > 1"));
    }

    @Test
    void formalHavingTemplatesExcludeExpressionGroupingProbe() {
        assertTrue(!TiDBVerticalPartitioningOracle.formalHavingTemplatesForTest(true)
                .contains(TiDBVerticalPartitioningOracle.HavingTemplate.GROUP_EXPRESSION_PROBE));
        assertTrue(!TiDBVerticalPartitioningOracle.formalHavingTemplatesForTest(false)
                .contains(TiDBVerticalPartitioningOracle.HavingTemplate.GROUP_EXPRESSION_PROBE));
    }

    @Test
    void viewJoinOnIsExcludedBeforeMaterialization() {
        EcstTarget target = new EcstTarget("TIDB_LOCATION_COP_TIKV_TO_ROOT", EcstContextDimension.LOCATION,
                "COP_TIKV", "ROOT");
        List<EcstTransformationConfig> candidates = TiDBECSTOracle.applicableConfigurations(target);
        assertTrue(candidates.stream().noneMatch(config -> config.getRepresentation()
                == EcstTransformationConfig.Representation.VIEW
                && config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.JOIN_ON));
    }

    @Test
    void enumeratesTheFullTransformationProductBeforeFiltering() {
        List<EcstTransformationConfig> configurations = TiDBECSTOracle.enumerateConfigurationSpace();

        assertEquals(4 * 2 * 4 * 7, configurations.size());
        assertTrue(configurations.stream().anyMatch(c -> c.getRepresentation()
                == EcstTransformationConfig.Representation.VIEW));
        assertTrue(configurations.stream().anyMatch(c -> c.getReconstruction()
                == EcstTransformationConfig.Reconstruction.CTE));
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.SELECT));
    }

    @Test
    void targetAndCapabilityFilteringProducesMoreThanTheOldFourConfigurations() {
        EcstTarget target = new EcstTarget("TIDB_LOCATION_SHIFT", EcstContextDimension.LOCATION, "*", "*");
        List<EcstTransformationConfig> configurations = TiDBECSTOracle.applicableConfigurations(target);

        assertTrue(configurations.size() > 4);
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.SELECT));
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.HAVING));
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.JOIN_ON));
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.INNER_WHERE));
        assertTrue(configurations.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.INNER_SELECT));
    }

    @Test
    void viewRepresentationHasAnExplicitMaterializationConfiguration() {
        List<EcstTransformationConfig> configurations = TiDBECSTOracle.applicableConfigurations(
                new EcstTarget("TIDB_LOCATION_SHIFT", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV"));
        assertTrue(configurations.stream().anyMatch(c -> c.getRepresentation()
                == EcstTransformationConfig.Representation.VIEW
                && c.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW));
        assertTrue(configurations.stream().anyMatch(c -> c.getRepresentation()
                == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                && c.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW));
        assertTrue(configurations.stream().anyMatch(c -> c.getRepresentation()
                == EcstTransformationConfig.Representation.VIEW
                && c.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE));
    }

    @Test
    void viewSemanticReferenceUsesTheViewNameInsteadOfPartitionNames() {
        assertTrue(TiDBVerticalPartitioningOracle.viewReferenceMatchesForTest(
                "SELECT `vp_rowid` FROM `vp_v` `source`", "vp_v"));
        assertTrue(!TiDBVerticalPartitioningOracle.viewReferenceMatchesForTest(
                "SELECT `vp_rowid` FROM `vp_l` JOIN `vp_r`", "vp_v"));
    }

    @Test
    void sourceReplayDropsQuotedSourceHistoryAndKeepsPartitionDependenciesOrdered() {
        List<String> replay = TiDBVerticalPartitioningOracle.assembleSourceDataReplayForTest(
                "vp_source", "vp_l", "vp_r", "vp_v", "vp_joined",
                List.of("CREATE TABLE `vp_source` (vp_rowid BIGINT PRIMARY KEY, c0 INT)"),
                List.of("INSERT INTO `vp_source` (vp_rowid, c0) VALUES (1, 1)"),
                List.of("CREATE TABLE `vp_source` (vp_rowid BIGINT PRIMARY KEY, c0 INT)",
                        "INSERT INTO `vp_source` (vp_rowid, c0) SELECT vp_rowid, c0 FROM `base_table`",
                        "ALTER TABLE `vp_source` ADD INDEX `source_idx` (`c0`)",
                        "CREATE TABLE `vp_l` (vp_rowid BIGINT PRIMARY KEY, c0 INT)",
                        "INSERT INTO `vp_l` SELECT vp_rowid, c0 FROM `vp_source`",
                        "CREATE TABLE `vp_r` (vp_rowid BIGINT PRIMARY KEY)",
                        "CREATE VIEW `vp_v` AS SELECT vp_rowid, c0 FROM `vp_l`"));
        assertTrue(replay.get(0).startsWith("CREATE TABLE `vp_source`"));
        assertTrue(replay.get(1).startsWith("INSERT INTO `vp_source`"));
        assertTrue(replay.stream().noneMatch(sql -> sql.toLowerCase().contains("base_table")));
        assertTrue(replay.stream().noneMatch(sql -> sql.toLowerCase().contains("alter table `vp_source`")));
        assertTrue(replay.indexOf("CREATE TABLE `vp_l` (vp_rowid BIGINT PRIMARY KEY, c0 INT)")
                < replay.indexOf("INSERT INTO `vp_l` SELECT vp_rowid, c0 FROM `vp_source`"));
    }

    @Test
    void ecstKeepsOnlyABoundedEmptyTableSamplingBudget() {
        assertEquals(0.05, TiDBVerticalPartitioningOracle.ecstEmptyBaseTableProbabilityForTest(), 0.000001);
    }

    @Test
    void resultSchemaMismatchIsRejectedEvenWhenRowsAreEmpty() {
        assertTrue(!TiDBVerticalPartitioningOracle.resultSchemasCompatibleForTest(
                List.of("BIGINT"), List.of("VARCHAR")));
        assertTrue(TiDBVerticalPartitioningOracle.resultSchemasCompatibleForTest(
                List.of("BIGINT", "VARCHAR"), List.of("BIGINT", "VARCHAR")));
    }

    @Test
    void targetDefinitionsCarryConcreteDirections() {
        List<EcstTarget> targets = TiDBECSTOracle.defaultTargets();

        assertTrue(targets.size() >= 6);
        assertTrue(targets.stream().noneMatch(target -> "*".equals(target.getFrom())
                || "*".equals(target.getTo())));
        assertTrue(targets.stream().anyMatch(target -> target.getDimension() == EcstContextDimension.LOCATION
                && "ROOT".equals(target.getFrom()) && "COP_TIKV".equals(target.getTo())));
        assertTrue(targets.stream().anyMatch(target -> target.getDimension() == EcstContextDimension.BOUNDARY
                && "NON_PUSHDOWN".equals(target.getFrom()) && "PUSHDOWN".equals(target.getTo())));
        assertTrue(targets.stream().anyMatch(target -> target.getDimension() == EcstContextDimension.OPERATOR
                && "SELECTION".equals(target.getFrom()) && "MERGEJOIN".equals(target.getTo())));
    }

    @Test
    void queryShapeUsesSqlClausesInsteadOfSchemaWidth() {
        TiDBECSTOracle.QueryShape plain = TiDBECSTOracle.queryShapeFromSql(
                "SELECT c0, c1 FROM t0 WHERE c0 > 0", 2);
        TiDBECSTOracle.QueryShape aggregate = TiDBECSTOracle.queryShapeFromSql(
                "SELECT c0, COUNT(*) FROM t0 GROUP BY c0 HAVING COUNT(*) > 1", 2);
        TiDBECSTOracle.QueryShape join = TiDBECSTOracle.queryShapeFromSql(
                "SELECT a.c0 FROM a JOIN b ON a.id = b.id", 2);

        assertTrue(!plain.hasAggregation() && !plain.hasJoin());
        assertTrue(aggregate.hasAggregation());
        assertTrue(join.hasJoin());
    }

    @Test
    void boundedCandidatesRetainMultipleDimensions() {
        List<EcstTransformationConfig> all = TiDBECSTOracle.applicableConfigurations(
                new EcstTarget("TIDB_LOCATION_SHIFT", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV"));
        assertTrue(all.stream().map(c -> c.getPartitionLayout()).distinct().count() >= 2);
        assertTrue(all.stream().map(c -> c.getReconstruction()).distinct().count() >= 2);
        assertTrue(all.stream().map(c -> c.getExpressionContext()).distinct().count() >= 2);
    }

    @Test
    void productionGeneratorShapeKeepsAllImplementedClausesReachable() {
        TiDBECSTOracle.QueryShape shape = TiDBECSTOracle.generatorQueryShape(true, 4);
        assertTrue(shape.hasProjection());
        assertTrue(shape.hasAggregation());
        assertTrue(!shape.hasJoin());
        EcstTarget target = new EcstTarget("TIDB_LOCATION_SHIFT", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV");
        List<EcstTransformationConfig> candidates = TiDBECSTOracle.applicableConfigurations(target);
        assertTrue(candidates.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.WHERE));
        assertTrue(candidates.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.SELECT));
        assertTrue(candidates.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.HAVING));
    }

    @Test
    void accessPathTargetRequiresAnExplicitPreparationPlan() {
        EcstTarget target = new EcstTarget("access", EcstContextDimension.ACCESS_PATH,
                "TABLEFULLSCAN", "INDEXLOOKUP");
        sqlancer.common.oracle.ecst.EcstContext context = new sqlancer.common.oracle.ecst.EcstContext(
                "ROOT", "SELECTION", "NON_PUSHDOWN", "TABLEFULLSCAN", "UNKNOWN");
        TiDBVerticalPartitioningOracle.EcstAccessPathPreparation unavailable =
                new TiDBVerticalPartitioningOracle.EcstAccessPathPreparation("source", List.of("c0"), false,
                        true, true);
        TiDBVerticalPartitioningOracle.EcstAccessPathPreparation available =
                new TiDBVerticalPartitioningOracle.EcstAccessPathPreparation("source", List.of("c0"), true,
                        true, true);
        assertTrue(!unavailable.supports(target, context, EcstTransformationConfig.ExpressionContext.WHERE));
        assertTrue(available.supports(target, context, EcstTransformationConfig.ExpressionContext.WHERE));
        assertTrue(!available.supports(target, context, EcstTransformationConfig.ExpressionContext.SELECT));
    }

    @Test
    void structuralApplicabilityRespectsMigrationDirectionAndDependencyLayout() {
        EcstTransformationConfig crossSideJoinWhere = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.CROSS_SIDE,
                EcstTransformationConfig.Reconstruction.JOIN,
                EcstTransformationConfig.ExpressionContext.WHERE);
        EcstTransformationConfig sameSideJoinWhere = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE,
                EcstTransformationConfig.Reconstruction.JOIN,
                EcstTransformationConfig.ExpressionContext.WHERE);
        EcstTarget rootToCop = new EcstTarget("root-cop", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV");
        EcstTarget copToRoot = new EcstTarget("cop-root", EcstContextDimension.LOCATION, "COP_TIKV", "ROOT");
        EcstTarget nonPushdownToPushdown = new EcstTarget("root-push", EcstContextDimension.BOUNDARY,
                "NON_PUSHDOWN", "PUSHDOWN");
        EcstTarget pushdownToNonPushdown = new EcstTarget("push-root", EcstContextDimension.BOUNDARY,
                "PUSHDOWN", "NON_PUSHDOWN");

        assertTrue(!TiDBECSTOracle.structurallyApplicableForTest(rootToCop, crossSideJoinWhere, "CROSS_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(TiDBECSTOracle.structurallyApplicableForTest(rootToCop, sameSideJoinWhere, "SAME_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(TiDBECSTOracle.structurallyApplicableForTest(copToRoot, crossSideJoinWhere, "CROSS_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(!TiDBECSTOracle.structurallyApplicableForTest(nonPushdownToPushdown, crossSideJoinWhere,
                "CROSS_SIDE", TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(TiDBECSTOracle.structurallyApplicableForTest(pushdownToNonPushdown, crossSideJoinWhere,
                "CROSS_SIDE", TiDBECSTOracle.QueryShape.synthetic()));
    }

    @Test
    void rootToCopRejectsRootOnlyEmbeddingContexts() {
        EcstTarget rootToCop = new EcstTarget("root-cop", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV");
        EcstTransformationConfig rootProjection = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE,
                EcstTransformationConfig.Reconstruction.JOIN,
                EcstTransformationConfig.ExpressionContext.SELECT);
        EcstTransformationConfig innerProjection = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE,
                EcstTransformationConfig.Reconstruction.JOIN,
                EcstTransformationConfig.ExpressionContext.INNER_SELECT);
        assertTrue(!TiDBECSTOracle.structurallyApplicableForTest(rootToCop, rootProjection, "SAME_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(TiDBECSTOracle.structurallyApplicableForTest(rootToCop, innerProjection, "SAME_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
    }

    @Test
    void concreteRootToCopTemplateUsesPredicateContexts() {
        EcstTarget rootToCop = new EcstTarget("TIDB_LOCATION_ROOT_TO_COP_TIKV", EcstContextDimension.LOCATION,
                "ROOT", "COP_TIKV");
        EcstTransformationConfig projectionGuard = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE,
                EcstTransformationConfig.Reconstruction.CTE,
                EcstTransformationConfig.ExpressionContext.SELECT);
        EcstTransformationConfig innerWhere = new EcstTransformationConfig(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE,
                EcstTransformationConfig.Reconstruction.CTE,
                EcstTransformationConfig.ExpressionContext.INNER_WHERE);
        assertTrue(!TiDBECSTOracle.structurallyApplicableForTest(rootToCop, projectionGuard, "SAME_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        assertTrue(TiDBECSTOracle.structurallyApplicableForTest(rootToCop, innerWhere, "SAME_SIDE",
                TiDBECSTOracle.QueryShape.synthetic()));
        List<EcstTransformationConfig> boundaryCandidates = TiDBECSTOracle.applicableConfigurations(
                new EcstTarget("TIDB_PUSHDOWN_ROOT_TO_COP", EcstContextDimension.BOUNDARY,
                        "NON_PUSHDOWN", "PUSHDOWN"));
        assertTrue(boundaryCandidates.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.SELECT));
        assertTrue(boundaryCandidates.stream().anyMatch(c -> c.getExpressionContext()
                == EcstTransformationConfig.ExpressionContext.INNER_SELECT));
    }

    @Test
    void directComparisonFilterCanPrepareAnAccessPath() {
        assertTrue(TiDBVerticalPartitioningOracle.isSargableAccessPredicateForTest("`c0` > 0", "c0"));
        assertTrue(!TiDBVerticalPartitioningOracle.isSargableAccessPredicateForTest(
                "CAST(`c0` AS SIGNED) + CAST(`c1` AS SIGNED) > 0", "c0"));
    }

    @Test
    void innerSelectPreservesSiblingProjectionColumns() {
        String target = "COALESCE(CAST(`c1` AS SIGNED), 0)";
        String original = "SELECT `vp_rowid`, " + target
                + " AS ecst_value, `c1` AS ecst_input FROM `source`";

        String rewritten = TiDBVerticalPartitioningOracle.rewriteInnerSelectProjection(original, target);

        assertEquals("SELECT `vp_rowid`, ecst_value AS ecst_value, `c1` AS ecst_input FROM `source`", rewritten);
    }

    @Test
    void concurrentJsonLinesAreWrittenAtomically() throws Exception {
        File directory = Files.createTempDirectory("ecst-json-log").toFile();
        ExecutorService workers = Executors.newFixedThreadPool(8);
        for (int worker = 0; worker < 8; worker++) {
            final int workerId = worker;
            workers.submit(() -> {
                for (int i = 0; i < 100; i++) {
                    TiDBECSTOracle.appendJsonLineForTest(directory, "cases.jsonl",
                            "{\"worker\":" + workerId + ",\"case\":" + i + "}");
                }
            });
        }
        workers.shutdown();
        assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        List<String> lines = Files.readAllLines(new File(directory, "cases.jsonl").toPath());
        assertEquals(800, lines.size());
        assertTrue(lines.stream().allMatch(line -> line.matches("\\{\\\"worker\\\":\\d+,\\\"case\\\":\\d+\\}")));
    }

    @Test
    void resultNormalizationKeepsStringSemanticsSeparateFromNumbers() {
        assertEquals("001", TiDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "VARCHAR"));
        assertEquals("1", TiDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "DECIMAL"));
        assertEquals("1", TiDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "BIGINT"));
        assertNotEquals(TiDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "VARCHAR"),
                TiDBVerticalPartitioningOracle.canonicalizeTypedValue("1", "VARCHAR"));
    }
}
