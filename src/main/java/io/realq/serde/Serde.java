package io.realq.serde;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Expr.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hive.hcatalog.data.JsonSerDe;


public class Serde {

    public static SerDe getSerde(Properties configSerde){

        SerDe serde = new JsonSerDe();
        try {
            serde.initialize(null, configSerde);
        } catch (SerDeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }  
        return serde;
    }
    
    public static Properties getSerdeProperties(List<Expr> exprs){
        List<String> newFields = new ArrayList<>();
        List<String> newFieldsTypes = new ArrayList<>();
        
        for(Expr expr:exprs){
            if(expr.category.equals(Expr.Category.FIELD_DEF)) {
                newFields.add(expr.getFieldDefValue().getName());
                newFieldsTypes.add(expr.getFieldDefValue().getType());
            } 
        }
        Properties configSerde = new Properties();
        configSerde.setProperty(Constants.LIST_COLUMNS,
                StringUtils.join(newFields.toArray(), ",")
                        .toLowerCase());
        configSerde.setProperty(Constants.LIST_COLUMN_TYPES, StringUtils
                .join(newFieldsTypes.toArray(), ",").toLowerCase());
        return configSerde;
    }
    
    public static List<Expr> getOutputFields(List<Expr> exprs){
        List<Expr> outputFields = new ArrayList<>();
        Properties configSerde = Serde.getSerdeProperties(exprs);
        SerDe serde = Serde.getSerde(configSerde);
        MessageParserPrims messageStructure = new MessageParserPrims(serde, null);

        for (String field : messageStructure.getFields()) {
            outputFields.add(new Expr(field, Category.FIELD));
        }
        return outputFields;
    }
    
}
