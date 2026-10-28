package sqlancer.common.oracle.ecst;

import java.util.Objects;

/**
 * A DBMS-independent description of the relation transformation controls.
 * DBMS adapters map these values to their native VP/reconstruction strategies.
 */
public final class EcstTransformationConfig {
    public enum Representation {
        ORIGINAL,
        VERTICAL_PARTITIONED,
        VIEW,
        REPLICATED
    }

    public enum PartitionLayout {
        SAME_SIDE,
        CROSS_SIDE
    }

    public enum Reconstruction {
        JOIN,
        VIEW,
        DERIVED_TABLE,
        CTE
    }

    public enum ExpressionContext {
        WHERE,
        SELECT,
        HAVING,
        ON
    }

    private final Representation representation;
    private final PartitionLayout partitionLayout;
    private final Reconstruction reconstruction;
    private final ExpressionContext expressionContext;

    public EcstTransformationConfig(Representation representation, PartitionLayout partitionLayout,
            Reconstruction reconstruction, ExpressionContext expressionContext) {
        this.representation = Objects.requireNonNull(representation);
        this.partitionLayout = Objects.requireNonNull(partitionLayout);
        this.reconstruction = Objects.requireNonNull(reconstruction);
        this.expressionContext = Objects.requireNonNull(expressionContext);
    }

    public Representation getRepresentation() {
        return representation;
    }

    public PartitionLayout getPartitionLayout() {
        return partitionLayout;
    }

    public Reconstruction getReconstruction() {
        return reconstruction;
    }

    public ExpressionContext getExpressionContext() {
        return expressionContext;
    }

    public String stableKey() {
        return representation + ":" + partitionLayout + ":" + reconstruction + ":" + expressionContext;
    }

    @Override
    public String toString() {
        return stableKey();
    }
}
