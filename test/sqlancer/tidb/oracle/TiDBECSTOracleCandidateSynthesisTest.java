package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;

public class TiDBECSTOracleCandidateSynthesisTest {
    @Test
    void declaresTheImplementedTidbPrototypeCapabilitySet() {
        TiDBEcstCapabilities capabilities = new TiDBEcstCapabilities();

        assertTrue(capabilities.supportsRepresentation(EcstTransformationConfig.Representation.VERTICAL_PARTITIONED));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.JOIN));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.DERIVED_TABLE));
        assertTrue(capabilities.supportsReconstruction(EcstTransformationConfig.Reconstruction.CTE));
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
    void directComparisonFilterCanPrepareAnAccessPath() {
        assertTrue(TiDBVerticalPartitioningOracle.isSargableAccessPredicateForTest("`c0` > 0", "c0"));
        assertTrue(!TiDBVerticalPartitioningOracle.isSargableAccessPredicateForTest(
                "CAST(`c0` AS SIGNED) + CAST(`c1` AS SIGNED) > 0", "c0"));
    }

    @Test
    void innerSelectPreservesOutputShapeIncludingSiblingColumn() {
        String target = "COALESCE(CAST(`c0` AS SIGNED), 0)";
        String original = "SELECT `vp_rowid`, " + target
                + " AS ecst_value, `c0` AS ecst_input FROM `source`";

        String rewritten = TiDBVerticalPartitioningOracle.rewriteInnerSelectProjection(original, target);

        assertEquals("SELECT `vp_rowid`, ecst_value AS ecst_value, `c0` AS ecst_input FROM `source`", rewritten);
        assertEquals(3, rewritten.substring(0, rewritten.indexOf(" FROM ")).split(",").length);
    }
}
