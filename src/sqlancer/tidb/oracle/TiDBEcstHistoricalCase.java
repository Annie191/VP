package sqlancer.tidb.oracle;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;

/**
 * A VP regression case expressed in ECST vocabulary.  The SQL files remain the
 * executable fixture; this descriptor records the context that the plan pair
 * is expected to exercise.  It is deliberately data-only so the normal ECST
 * feedback loop can use the same target/configuration keys as generated cases.
 */
public final class TiDBEcstHistoricalCase {
    public static final class ObservedTransition {
        private final EcstContextDimension dimension;
        private final String from;
        private final String to;
        private final String evidence;

        private ObservedTransition(EcstContextDimension dimension, String from, String to, String evidence) {
            this.dimension = dimension;
            this.from = from;
            this.to = to;
            this.evidence = evidence;
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

        public String getEvidence() {
            return evidence;
        }

        public String targetKey() {
            String family;
            switch (dimension) {
            case LOCATION:
                family = "LOCATION";
                break;
            case BOUNDARY:
                family = "PUSHDOWN";
                break;
            case ACCESS_PATH:
                family = "ACCESS";
                break;
            default:
                family = dimension.name();
                break;
            }
            String source = dimension == EcstContextDimension.BOUNDARY ? boundaryLocation(from) : from;
            String destination = dimension == EcstContextDimension.BOUNDARY ? boundaryLocation(to) : to;
            return "TIDB_" + family + "_" + source + "_TO_" + destination;
        }

        private static String boundaryLocation(String value) {
            if ("PUSHDOWN".equals(value)) {
                return "COP";
            }
            if ("NON_PUSHDOWN".equals(value)) {
                return "ROOT";
            }
            return value;
        }
    }
    private final String issue;
    private final String family;
    private final EcstTransformationConfig.ExpressionContext context;
    private final EcstTransformationConfig.Reconstruction reconstruction;
    private final EcstTransformationConfig.PartitionLayout layout;
    private final EcstContextDimension dimension;
    private final String from;
    private final String to;
    private final String operatorFrom;
    private final String operatorTo;
    private final List<String> evidence;

    private TiDBEcstHistoricalCase(String issue, String family,
            EcstTransformationConfig.ExpressionContext context,
            EcstTransformationConfig.Reconstruction reconstruction,
            EcstTransformationConfig.PartitionLayout layout, EcstContextDimension dimension,
            String from, String to, String operatorFrom, String operatorTo, String... evidence) {
        this.issue = issue;
        this.family = family;
        this.context = context;
        this.reconstruction = reconstruction;
        this.layout = layout;
        this.dimension = dimension;
        this.from = from;
        this.to = to;
        this.operatorFrom = operatorFrom;
        this.operatorTo = operatorTo;
        this.evidence = Collections.unmodifiableList(Arrays.asList(evidence));
    }

    public String getIssue() {
        return issue;
    }

    public String getFamily() {
        return family;
    }

    public EcstTransformationConfig.ExpressionContext getContext() {
        return context;
    }

    public EcstTransformationConfig.Reconstruction getReconstruction() {
        return reconstruction;
    }

    public EcstTransformationConfig.PartitionLayout getLayout() {
        return layout;
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

    public String getOperatorFrom() {
        return operatorFrom;
    }

    public String getOperatorTo() {
        return operatorTo;
    }

    public List<String> getEvidence() {
        return evidence;
    }

    /**
     * All independently visible context changes in the captured plan pair.
     * The first transition is the case's primary target; the rest are
     * secondary evidence and are never used to replace the primary target.
     */
    public List<ObservedTransition> observedTransitions() {
        List<ObservedTransition> result = new ArrayList<>();
        result.add(new ObservedTransition(dimension, from, to, String.join("; ", evidence)));
        switch (issue) {
        case "69178":
        case "69191":
        case "69292":
        case "69296":
        case "69308":
        case "69358":
            result.add(new ObservedTransition(EcstContextDimension.OPERATOR, "HASHAGG", "PROJECTION",
                    "target expression is in cop HashAgg before reconstruction and root Projection after it"));
            result.add(new ObservedTransition(EcstContextDimension.BOUNDARY, "PUSHDOWN", "NON_PUSHDOWN",
                    "aggregate expression crosses the cop/root evaluation boundary"));
            break;
        case "69294":
        case "69295":
        case "69484":
            result.add(new ObservedTransition(EcstContextDimension.OPERATOR, "SELECTION", "MERGEJOIN",
                    "target predicate is represented by Join.other cond after reconstruction"));
            result.add(new ObservedTransition(EcstContextDimension.BOUNDARY, "PUSHDOWN", "NON_PUSHDOWN",
                    "predicate evaluation moves from cop Selection into the root join"));
            break;
        case "69480":
            result.add(new ObservedTransition(EcstContextDimension.OPERATOR, "TOPN", "SORT",
                    "ordering is implemented by cop TopN before and root Sort after reconstruction"));
            result.add(new ObservedTransition(EcstContextDimension.BOUNDARY, "PUSHDOWN", "NON_PUSHDOWN",
                    "ordering boundary crosses from cop to root"));
            break;
        case "69482":
            result.add(new ObservedTransition(EcstContextDimension.BOUNDARY, "PUSHDOWN", "NON_PUSHDOWN",
                    "TopN moves from cop below the view to root above the join"));
            break;
        case "69598":
            result.add(new ObservedTransition(EcstContextDimension.BOUNDARY, "NON_PUSHDOWN", "PUSHDOWN",
                    "correlated rowid condition is pushed from root HashJoin into inner cop Selections"));
            break;
        default:
            break;
        }
        return Collections.unmodifiableList(result);
    }

    public String targetKey() {
        return new ObservedTransition(dimension, from, to, "").targetKey();
    }

    public String configKey() {
        return EcstTransformationConfig.Representation.VERTICAL_PARTITIONED + ":" + layout + ":"
                + reconstruction + ":" + context;
    }

    /** The 16 historical VP differences currently used as ECST seed fixtures. */
    public static List<TiDBEcstHistoricalCase> all() {
        return List.of(
                c("69177", "CAST_BINARY_MIN", "SELECT", "JOIN", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "Projection", "Projection", "cast + MIN pushed to cop"),
                c("69178", "TEMPORAL_CASE_COUNT", "SELECT", "JOIN", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "CASE + TIME/DATETIME cast"),
                c("69191", "BINARY_CASE_MIN_VIEW", "SELECT", "VIEW", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "view reconstruction"),
                c("69292", "TEMPORAL_CASE_COUNT_DERIVED", "SELECT", "DERIVED_TABLE", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "derived-table reconstruction"),
                c("69293", "BINARY_OR_GROUP_FILTER", "WHERE", "DERIVED_TABLE", "SAME_SIDE", "OPERATOR", "SELECTION", "SELECTION", "SELECTION", "SELECTION", "predicate remains in cop on both inputs"),
                c("69294", "CASE_HAVING_JOIN_CONDITION", "HAVING", "DERIVED_TABLE", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "SELECTION", "MERGEJOIN", "CASE predicate becomes Join.other cond"),
                c("69295", "DATE_FORMAT_CASE", "WHERE", "DERIVED_TABLE", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "SELECTION", "MERGEJOIN", "DATE_FORMAT expression in Join.other cond"),
                c("69296", "TEMPORAL_CASE_MAX_SCALAR", "SELECT", "DERIVED_TABLE", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "scalar reconstruction"),
                c("69308", "TEMPORAL_CASE_BIT_LENGTH", "SELECT", "JOIN", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "aggregate expression lifted above join"),
                c("69354", "ROWID_BINARY_BOUNDARY", "WHERE", "JOIN", "SAME_SIDE", "OPERATOR", "SELECTION", "SELECTION", "SELECTION", "SELECTION", "nested join preserves root selection"),
                c("69357", "NULL_SAFE_CASE_AGG", "WHERE", "JOIN", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "SELECTION", "SELECTION", "selection lifted above outer join"),
                c("69358", "GROUP_CASE_TEMPORAL", "SELECT", "JOIN", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "HASHAGG", "PROJECTION", "group expression reconstructed above join"),
                c("69480", "NULL_ORDER_INDEX_READER", "WHERE", "JOIN", "SAME_SIDE", "ACCESS_PATH", "INDEXREADER", "TABLEFULLSCAN", "INDEXREADER", "MERGEJOIN", "IndexReader/order boundary"),
                c("69482", "STRING_MIN_VIEW", "SELECT", "VIEW", "SAME_SIDE", "LOCATION", "COP_TIKV", "ROOT", "TOPN", "TOPN", "string MIN and view reconstruction"),
                c("69484", "INET_CASE_CORRELATED", "WHERE", "DERIVED_TABLE", "CROSS_SIDE", "LOCATION", "COP_TIKV", "ROOT", "SELECTION", "MERGEJOIN", "correlated split predicate"),
                c("69598", "CORRELATED_MIN_APPLY", "SELECT", "CTE", "CROSS_SIDE", "OPERATOR", "HASHJOIN", "APPLY", "HASHJOIN", "APPLY", "correlated MIN becomes Apply"));
    }

    private static TiDBEcstHistoricalCase c(String issue, String family, String context, String reconstruction,
            String layout, String dimension, String from, String to, String operatorFrom, String operatorTo,
            String... evidence) {
        return new TiDBEcstHistoricalCase(issue, family,
                EcstTransformationConfig.ExpressionContext.valueOf(context),
                EcstTransformationConfig.Reconstruction.valueOf(reconstruction),
                EcstTransformationConfig.PartitionLayout.valueOf(layout), EcstContextDimension.valueOf(dimension),
                from, to, operatorFrom, operatorTo, evidence);
    }
}
