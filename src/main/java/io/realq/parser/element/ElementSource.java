package io.realq.parser.element;

import io.realq.parser.expr.Expr;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ElementSource extends Element {

    private String sourceClass;
    private List<Expr> fieldDefs = new ArrayList<Expr>();
    private Properties sourceProperties = new Properties();

    public void setSourceClass(String sourceClass) {
        this.sourceClass = sourceClass;
    }

    public void setSourceProperty(String key, String value) {
        sourceProperties.setProperty(key, value);
    }

    public boolean addFieldDef(Expr e) {
        return fieldDefs.add(e);
    }

    public String getSourceClass() {
        return sourceClass;
    }

    public List<Expr> getFieldDefs() {
        return fieldDefs;
    }

    public Properties getSourceProperties() {
        return sourceProperties;
    }

}
