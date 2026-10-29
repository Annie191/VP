package sqlancer.common.oracle.ecst;

/** Structured session environment evidence associated with an execution context. */
public final class EcstEvaluationEnvironment {
    private final String sqlMode;
    private final String collation;
    private final String characterSet;
    private final String typeConversion;
    private final boolean evidenceAvailable;

    public EcstEvaluationEnvironment(String sqlMode, String collation, String characterSet, String typeConversion,
            boolean evidenceAvailable) {
        this.sqlMode = value(sqlMode);
        this.collation = value(collation);
        this.characterSet = value(characterSet);
        this.typeConversion = value(typeConversion);
        this.evidenceAvailable = evidenceAvailable;
    }

    public static EcstEvaluationEnvironment unavailable() {
        return new EcstEvaluationEnvironment("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", false);
    }

    private static String value(String value) {
        return value == null || value.trim().isEmpty() ? "UNKNOWN" : value.trim();
    }

    public String getSqlMode() { return sqlMode; }
    public String getCollation() { return collation; }
    public String getCharacterSet() { return characterSet; }
    public String getTypeConversion() { return typeConversion; }
    public boolean isEvidenceAvailable() { return evidenceAvailable; }

    @Override
    public String toString() {
        return evidenceAvailable ? "sql_mode=" + sqlMode + ";collation=" + collation + ";charset="
                + characterSet + ";targetTypeConversion=" + typeConversion : "EVIDENCE_UNAVAILABLE";
    }
}
