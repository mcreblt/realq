package io.realq.serde;

import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

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

public class MessageParserPrims {

    Boolean isResult;
    private Map<String, Object> primitives = new HashMap<>();
    
    public MessageParserPrims(SerDe serde, Object result){
        this.isResult = result==null ? false : true;

        try {
            inspectRoot(serde, result);
        } catch (SerDeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public List<String> getFields(){

        return new ArrayList<String>(primitives.keySet());
        
    }
    public Object getFieldData(String field) {
        return primitives.get(field);
    }

    private void inspectRoot(SerDe serde, Object result) throws SerDeException {
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
    
    private Primitive toPrimitive(PrimitiveCategory category, StructObjectInspector soi, Object result, StructField field){
        
        if(isResult == false) {
            return null;
        } else {
            return Types.toPrimitive(soi.getStructFieldData(result, field), category);
        }
        
    }
    private void inspectPrimitiveField(StructField field, Object result, StructObjectInspector soi, String fieldName) {
        PrimitiveObjectInspector primitiveOI = (PrimitiveObjectInspector) field.getFieldObjectInspector();
        PrimitiveCategory category = primitiveOI.getPrimitiveCategory();

        primitives.put(fieldName, toPrimitive(category, soi, result, field));        
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
