package sqlancer.common.oracle.ecst;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

/** Shared structural SQL expression fingerprints; adapters must separately establish physical lineage. */
public final class EcstExpressionMatcher {
    private EcstExpressionMatcher() {
    }

    public static String fingerprint(String expression) {
        try {
            return key(CCJSqlParserUtil.parseExpression(expression));
        } catch (JSQLParserException | RuntimeException unavailable) {
            return null;
        }
    }

    public static int countMatches(Expression expression, String target) {
        if (expression instanceof Parenthesis) {
            return countMatches(((Parenthesis) expression).getExpression(), target);
        }
        int count = target.equals(key(expression)) ? 1 : 0;
        for (Expression child : children(expression)) {
            count += countMatches(child, target);
        }
        return count;
    }

    private static List<Expression> children(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            return List.of(binary.getLeftExpression(), binary.getRightExpression());
        }
        if (expression instanceof CastExpression) {
            return List.of(((CastExpression) expression).getLeftExpression());
        }
        if (expression instanceof SignedExpression) {
            return List.of(((SignedExpression) expression).getExpression());
        }
        if (expression instanceof Parenthesis) {
            return List.of(((Parenthesis) expression).getExpression());
        }
        if (expression instanceof IsNullExpression) {
            return List.of(((IsNullExpression) expression).getLeftExpression());
        }
        if (expression instanceof WhenClause) {
            WhenClause when = (WhenClause) expression;
            return List.of(when.getWhenExpression(), when.getThenExpression());
        }
        if (expression instanceof CaseExpression) {
            CaseExpression caseExpression = (CaseExpression) expression;
            List<Expression> result = new ArrayList<>();
            if (caseExpression.getSwitchExpression() != null) {
                result.add(caseExpression.getSwitchExpression());
            }
            if (caseExpression.getWhenClauses() != null) {
                result.addAll(caseExpression.getWhenClauses());
            }
            if (caseExpression.getElseExpression() != null) {
                result.add(caseExpression.getElseExpression());
            }
            return result;
        }
        if (expression instanceof Function && ((Function) expression).getParameters() != null) {
            return ((Function) expression).getParameters().getExpressions();
        }
        return List.of();
    }

    private static String key(Expression expression) {
        if (expression instanceof Parenthesis) {
            return key(((Parenthesis) expression).getExpression());
        }
        if (expression instanceof Column) {
            return "column:" + ((Column) expression).getColumnName().replace("`", "").toLowerCase(Locale.ROOT);
        }
        if (expression instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            String operator = expression.getClass().getSimpleName();
            String left = key(binary.getLeftExpression());
            String right = key(binary.getRightExpression());
            if ("GreaterThan".equals(operator) || "GreaterThanEquals".equals(operator)) {
                operator = "GreaterThan".equals(operator) ? "MinorThan" : "MinorThanEquals";
                String swap = left;
                left = right;
                right = swap;
            }
            return operator + "[" + left + "," + right + "]";
        }
        if (expression instanceof Function) {
            Function function = (Function) expression;
            return "function:" + function.getName().toLowerCase(Locale.ROOT) + ":" + function.isDistinct() + "["
                    + children(expression).stream().map(EcstExpressionMatcher::key).collect(Collectors.joining(","))
                    + "]";
        }
        if (expression instanceof CastExpression) {
            CastExpression cast = (CastExpression) expression;
            return "cast:" + cast.getType().toString().toLowerCase(Locale.ROOT).replaceAll("\\s+", "") + "["
                    + key(cast.getLeftExpression()) + "]";
        }
        if (expression instanceof SignedExpression) {
            SignedExpression signed = (SignedExpression) expression;
            return signed.getSign() + "[" + key(signed.getExpression()) + "]";
        }
        if (expression instanceof CaseExpression || expression instanceof WhenClause
                || expression instanceof IsNullExpression) {
            String negation = expression instanceof IsNullExpression && ((IsNullExpression) expression).isNot() ? "not"
                    : "";
            return expression.getClass().getSimpleName() + negation + "["
                    + children(expression).stream().map(EcstExpressionMatcher::key).collect(Collectors.joining(","))
                    + "]";
        }
        return expression.getClass().getSimpleName() + ":" + expression.toString();
    }
}
