package io.realq.parser.expr;

import java.math.BigDecimal;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.WritableComparator;
import org.joda.time.DateTime;

public class Types {
    static public enum Type {
        UNKNOWN, NULL, BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DATETIME, BYTEARRAY, STRING, BIGDECIMAL, ERROR
    }

    public static Type getType(Object o) {

        if (o == null) {
            return Type.NULL;
        }

        if (o instanceof Byte[]) {
            return Type.BYTEARRAY;
        } else if (o instanceof String) {
            return Type.STRING;
        } else if (o instanceof Integer) {
            return Type.INTEGER;
        } else if (o instanceof Long) {
            return Type.LONG;
        } else if (o instanceof Float) {
            return Type.FLOAT;
        } else if (o instanceof Double) {
            return Type.DOUBLE;
        } else if (o instanceof Boolean) {
            return Type.BOOLEAN;
        } else if (o instanceof DateTime) {
            return Type.DATETIME;
        } else if (o instanceof BigDecimal) {
            return Type.BIGDECIMAL;
        } else {
            return Type.ERROR;
        }
    }

    public static boolean isNumeric(Type type) {

        return type == Type.INTEGER 
            || type == Type.LONG 
            || type == Type.FLOAT 
            || type == Type.DOUBLE
            || type == Type.BIGDECIMAL;
    }
    
    public static Primitive toBoolPrimitive(Object o){
        return new Primitive((Boolean) o, Type.BOOLEAN);
    }
    
    public static Primitive toIntPrimitive(Object o){
        return new Primitive((Integer) o, Type.INTEGER);
    }
    
    public static Primitive toLongPrimitive(Object o){
        return new Primitive((Long) o, Type.LONG);
    }
    
    public static Primitive toFloatPrimitive(Object o){
        return new Primitive((Float) o, Type.FLOAT);
    }
    
    public static Primitive toDoublePrimitive(Object o){
        return new Primitive((Double) o, Type.DOUBLE);
    }

    public static Primitive toBigDecimalPrimitive(Object o){
        return new Primitive(new BigDecimal(o.toString()), Type.BIGDECIMAL);
    }
    public static Primitive toByteArrayPrimitive(Object o){
        return new Primitive((byte[]) o, Type.STRING);
    }
    public static Primitive toStringPrimitive(Object o){
        return new Primitive((String) o, Type.STRING);
    }
    
    public static int compare(Object o1, Type dt1, Object o2, Type dt2) {
        if (dt1 == dt2) {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                if (o2 == null) {
                    return 1;
                }
            }
            
            switch (dt1) {
            case NULL:
                return 0;

            case BOOLEAN:
                return ((Boolean) o1).compareTo((Boolean) o2);

            case INTEGER:
                return ((Integer) o1).compareTo((Integer) o2);

            case LONG:
                return ((Long) o1).compareTo((Long) o2);

            case FLOAT:
                return ((Float) o1).compareTo((Float) o2);

            case DOUBLE:
                return ((Double) o1).compareTo((Double) o2);

            case BIGDECIMAL:
                return ((BigDecimal) o1).compareTo((BigDecimal) o2);
                
            case DATETIME:
                return ((DateTime) o1).compareTo((DateTime) o2);
                
            case BYTEARRAY:
                byte[] o1b = (byte[]) o1;
                byte[] o2b = (byte[]) o2;
                return WritableComparator.compareBytes(
                        o1b, 0, o1b.length,
                        o2b, 0, o2b.length);

            case STRING:
                return ((String) o1).compareTo((String) o2);

            default:
                throw new RuntimeException("Compare - Unkown type " + dt1);
            }
        } else {
            if(isNumeric(dt1)){
                return (new BigDecimal(o1.toString())).compareTo(new BigDecimal(o1.toString()));
            } else if (dt1 == Type.STRING) {
                return ((String) o1).compareTo(o2.toString());
            } else {
                return 1;    
            }
        }
    }

    public static Primitive toPrimitive(Object o, PrimitiveCategory category) {
        Primitive primitive;
        switch (category){
        case BOOLEAN:
            primitive = Types.toBoolPrimitive(o);
            break;
        case INT:
            primitive = Types.toIntPrimitive(o);
            break;
        case LONG:
            primitive = Types.toLongPrimitive(o);
            break;
        case FLOAT:
            primitive = Types.toFloatPrimitive(o);
            break;
        case DOUBLE:
            primitive = Types.toDoublePrimitive(o);
            break;
        case DECIMAL:
            primitive = Types.toBigDecimalPrimitive(o);
            break;
        case BYTE:
            primitive = Types.toByteArrayPrimitive(o);
            break;
        case STRING:
            primitive = Types.toStringPrimitive(o);
            break;
        default:
            primitive = new Primitive(null, Types.Type.NULL);
            break;
        }
        return primitive;
    }
}
