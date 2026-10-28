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
}
