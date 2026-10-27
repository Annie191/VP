package sqlancer.common.oracle.ecst;

import java.util.Objects;

/** A requested transition in one execution-context dimension. */
public final class EcstTarget {
    private final String key;
    private final EcstContextDimension dimension;
    private final String from;
    private final String to;

    public EcstTarget(String key, EcstContextDimension dimension, String from, String to) {
        this.key = Objects.requireNonNull(key);
        this.dimension = Objects.requireNonNull(dimension);
        this.from = Objects.requireNonNull(from);
        this.to = Objects.requireNonNull(to);
    }

    public String getKey() {
        return key;
    }

    public EcstContextDimension getDimension() {
        return dimension;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    @Override
    public String toString() {
        return key + "(" + dimension + ":" + from + "->" + to + ")";
    }
}
