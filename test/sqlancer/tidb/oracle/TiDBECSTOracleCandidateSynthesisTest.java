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
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.WHERE));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.SELECT));
        assertTrue(capabilities.supportsExpressionContext(EcstTransformationConfig.ExpressionContext.HAVING));
        assertEquals("TiDB ECST prototype capability set", capabilities.describe());
    }

    @Test
    void enumeratesTheFullTransformationProductBeforeFiltering() {
        List<EcstTransformationConfig> configurations = TiDBECSTOracle.enumerateConfigurationSpace();

        assertEquals(4 * 2 * 4 * 4, configurations.size());
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
}
