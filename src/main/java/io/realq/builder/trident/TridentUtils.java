package io.realq.builder.trident;

import io.realq.parser.expr.Expr;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.WordUtils;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;

public class TridentUtils {

    protected static Function getFuntion(String function, List<Expr> exprs) {
        storm.trident.operation.Function baseFunction = null;
        Class<?> functionClass;
        try {
            functionClass = Class.forName("io.realq.builder.trident.operation."
                    + WordUtils.capitalizeFully(function.toLowerCase(), new char[] { '_' }).replaceAll("_", ""));
            Constructor<?> ctor = functionClass.getConstructor(List.class);

            baseFunction = (BaseFunction) ctor.newInstance(new Object[] { exprs });
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return baseFunction;
    }
    

    protected static Filter getFilter(String function, List<Expr> exprs) {
        BaseFilter baseFunction = null;
        Class<?> functionClass;
        try {
            functionClass = Class.forName("io.realq.builder.trident.operation."
                    + WordUtils.capitalizeFully(function.toLowerCase(), new char[] { '_' }).replaceAll("_", ""));
            Constructor<?> ctor = functionClass.getConstructor(List.class);

            baseFunction = (BaseFilter) ctor.newInstance(new Object[] { exprs });
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return baseFunction;
    }
    
    protected static CombinerAggregator<Long> getFunction(String function) {
        CombinerAggregator<Long> baseFunction = null;
        Class<?> functionClass;
        try {
            functionClass = Class.forName("io.realq.builder.trident.aggr."
                    + WordUtils.capitalizeFully(function.toLowerCase(), new char[] { '_' }).replaceAll("_", ""));
            // Constructor<?> ctor = functionClass.getConstructor(List.class);

            baseFunction = (CombinerAggregator<Long>) functionClass.newInstance();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return baseFunction;
    }
    
    public static List<String> getFields(List<Expr> exprs) {
        ArrayList<String> fields = new ArrayList<String>();
        for (Expr expr : exprs) {
            if (expr.category.equals(Expr.Category.FIELD)) {
                fields.add(expr.getStringValue());
            }
        }
        return fields;
    }
}
