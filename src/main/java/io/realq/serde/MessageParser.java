package io.realq.serde;

import io.realq.parser.expr.Primitive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class MessageParser {
    
    Map<String, Primitive> primitives = new HashMap<String, Primitive>();
    SerDe serde;
    Object result;
    
    public MessageParser(SerDe serde, Object result){
        
        this.serde = serde;
        this.result = result;
        try {
            inspectRoot();
        } catch (SerDeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public List<String> getFields(){

        return new ArrayList<String>(primitives.keySet());
        
    }
    public Primitive getFieldData(String field) {
        return primitives.get(field);
    }

    private void inspectRoot() throws SerDeException {
        Category topCategory = serde.getObjectInspector().getCategory();
        
        if(topCategory.equals(Category.STRUCT)) {
            StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
            
            for(StructField field: soi.getAllStructFieldRefs()){
                                     
                if(field.getFieldObjectInspector().getCategory().equals(Category.PRIMITIVE)){
                    inspectPrimitiveField(field, result, soi, field.getFieldName());
                }
                
                if(field.getFieldObjectInspector().getCategory().equals(Category.STRUCT)){
                    inspectStructField(field, result, soi, field.getFieldName());
                }
            }
        }
    }

    private void inspectPrimitiveField(StructField field, Object result, StructObjectInspector soi, String prefix) {
        PrimitiveObjectInspector primitive = (PrimitiveObjectInspector) field.getFieldObjectInspector();
        String fieldName = prefix;
        if(primitive.getPrimitiveCategory().equals(PrimitiveCategory.INT)){
            Long dataLong = result == null ?  null : Long.parseLong(soi.getStructFieldData(result, field).toString());
            primitives.put(fieldName, new Primitive(dataLong));
        }
        if(primitive.getPrimitiveCategory().equals(PrimitiveCategory.DOUBLE)){
            Double data = result == null ?  null : Double.parseDouble(soi.getStructFieldData(result, field).toString());
            primitives.put(fieldName, new Primitive(data));
        }
        
        if(primitive.getPrimitiveCategory().equals(PrimitiveCategory.STRING)){
            String data = result == null ?  null : soi.getStructFieldData(result, field).toString();
            primitives.put(fieldName, new Primitive(data));
        }
        if(primitive.getPrimitiveCategory().equals(PrimitiveCategory.BOOLEAN)){
            Boolean data = result == null ?  null : Boolean.parseBoolean(soi.getStructFieldData(result, field).toString());
            primitives.put(fieldName, new Primitive(data));
        }
    }
    
    private void inspectStructField(StructField field, Object result, StructObjectInspector soi, String prefix) {
        StructObjectInspector struc = (StructObjectInspector) field.getFieldObjectInspector();
        Object strucData = soi.getStructFieldData(result, field);

        for(StructField childField: struc.getAllStructFieldRefs()){
            
            String fieldName = prefix+"."+childField.getFieldName();

            if(childField.getFieldObjectInspector().getCategory().equals(Category.PRIMITIVE)){
                inspectPrimitiveField(childField, strucData, struc, fieldName);
            }
            
            if(childField.getFieldObjectInspector().getCategory().equals(Category.STRUCT)){
                inspectStructField(childField, strucData, struc, fieldName);
            }
        }
    }
}
