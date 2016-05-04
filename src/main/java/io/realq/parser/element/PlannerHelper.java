package io.realq.parser.element;

import org.apache.commons.lang.math.NumberUtils;

import io.realq.query.antlr4.QueryParser.SelectExprContext;

public class PlannerHelper {

    public static String alias(SelectExprContext selectExpr) {
        // whether alias or not
        String alias;
        if (selectExpr.alias() != null) {
            alias = selectExpr.alias().getText();
        } else {
            alias = selectExpr.getText();
        }
        return alias;
    }

    public static String stripQuotes(String string) {
        return string.substring(1, string.length() - 1);
    }

    public static Number parseNumeric(String value) {
        Number result = null;
        try {
            result = NumberUtils.createNumber(value);
        } catch (NumberFormatException e) {
            System.out.println("Unsupported numeric: " + value);
        }
        return result;
    }
}
