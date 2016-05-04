package io.realq.parser.expr;

import java.io.Serializable;
import java.util.Properties;

public class Expr implements Serializable {

    private static final long serialVersionUID = 30334224838003772L;
    public Object value;
    public Category category;

    static public enum Category {
        FIELD, FIELD_DEF, OPERATOR, LITERAL, OPERATOR_LOGICAL
    }

    public Expr(Object value, Category type) {
        super();
        this.value = value;
        this.category = type;
    }

    public String getStringValue() {
        return value.toString();
    }

    public Primitive getPrimitiveValue() {
        return (Primitive) value;
    }

    public Properties getProperiesValue() {
        return (Properties) value;
    }
    
    public FieldDef getFieldDefValue() {
        return (FieldDef) value;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((category == null) ? 0 : category.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Expr other = (Expr) obj;
        if (category != other.category)
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Expr [value=" + value + ", type=" + category + "]";
    }

}
