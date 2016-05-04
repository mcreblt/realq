package io.realq.builder.trident.test.utils;

import backtype.storm.LocalDRPC;

public class FieldTestDef {
    private String idField;
    private String id;
    private String testField;
    private String expected;
    private LocalDRPC state;
    public FieldTestDef(String idField, String id, String testField, String expected) {
        super();
        this.idField = idField;
        this.id = id;
        this.testField = testField;
        this.expected = expected;
    }
    public String getIdField() {
        return idField;
    }
    public String getId() {
        return id;
    }
    public String getTestField() {
        return testField;
    }
    public String getExpected() {
        return expected;
    }
    public LocalDRPC getState() {
        return state;
    }
    public void setState(LocalDRPC state) {
        this.state = state;
    }
    
}
