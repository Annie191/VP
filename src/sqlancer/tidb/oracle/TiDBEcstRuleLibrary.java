package sqlancer.tidb.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.oracle.ecst.EcstHistoricalRule;

/** TiDB-specific evidence layer over the DBMS-independent ECST rule library. */
public final class TiDBEcstRuleLibrary {
    private TiDBEcstRuleLibrary() {
    }

    public static List<EcstHistoricalRule> historicalRules() {
        List<EcstHistoricalRule> result = new ArrayList<>();
        for (TiDBEcstHistoricalCase historical : TiDBEcstHistoricalCase.all()) {
            for (TiDBEcstHistoricalCase.ObservedTransition transition : historical.observedTransitions()) {
                result.add(new EcstHistoricalRule(
                        "TiDB historical VP difference " + historical.getIssue() + " "
                                + transition.getDimension(),
                        historical.getFamily(), transition.targetKey(), historical.configKey(),
                        List.of(historical.getIssue()), List.of(transition.getEvidence()),
                        sqlancer.common.oracle.ecst.EcstHistoricalRule.Kind.valueOf(
                                transition.getKind().name())));
            }
        }
        return result;
    }
}
