package com.maxdemarzi.schema;

public final class Properties {

    private Properties() {
        throw new IllegalAccessError("Utility class");
    }

    public static final String NAME = "name";
    public static final String NPI = "npi";
    public static final String FIRST_NAME = "first_name";
    public static final String LAST_NAME = "last_name";

    public static final String BENE_COUNT  = "bene_count";
    public static final String TOTAL_CLAIM_COUNT  = "total_claim_count";
    public static final String TOTAL_30_DAY_FILL_COUNT = "total_30_day_fill_count";
    public static final String TOTAL_DAY_SUPPLY = "total_day_supply";
    public static final String TOTAL_DRUG_COST = "total_drug_cost";

    public static final String BENE_COUNT_GE65 = "bene_count_ge65";
    public static final String TOTAL_CLAIM_COUNT_GE65 = "total_claim_count_ge65";
    public static final String TOTAL_30_DAY_FILL_COUNT_GE65 = "total_30_day_fill_count_ge65";
    public static final String TOTAL_DAY_SUPPLY_GE65 = "total_day_supply_ge65";
    public static final String TOTAL_DRUG_COST_GE65  = "total_drug_cost_ge65";

}