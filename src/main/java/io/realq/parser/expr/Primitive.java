package io.realq.parser.expr;

import io.realq.parser.expr.Types.Type;

import java.io.Serializable;
import java.math.BigDecimal;

public class Primitive implements Serializable, Comparable<Primitive> {

    private static final long serialVersionUID = -1510575648231736008L;
    
    Object value;
    Type type;

    public Primitive(Object value) {
        super();

        this.value = value;
        this.type = Types.getType(value);
    }
    public Primitive(Object value, Type type) {
        super();

        this.value = value;
        this.type = type;
    }

    public Type getType() {
        return type;
    }
    
    @Override
    public String toString() {
        return type + ":" + value;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        Primitive other = (Primitive) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public int compareTo(Primitive other) {
        return Types.compare(this.value, this.type, other.getValue(), other.getType());
    }
    
    public Number getNumber() {
        return (Number) this.value;
    }
    public Integer getInteger() {
        return (Integer) this.value;
    }
    public Long getLong() {
        return (Long) this.value;
    }
    public Float getFloat() {
        return (Float) this.value;
    }
    public Double getDouble() {
        return (Double) this.value;
    }
    public BigDecimal getBigDecimal() {
        return (BigDecimal) this.value;
    }
    public Boolean getBoolean() {
        return (Boolean) this.value;
    }
    public String getString() {
        return (String) this.value;
    }
    public String toStringValue() {
        return this.value.toString();
    }
    public Boolean toBooleanValue() {
        return Boolean.valueOf(this.value.toString());
    }
}