package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.serde.MessageParser;
import io.realq.serde.MessageParserPrims;
import io.realq.serde.Serde;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Deserialize extends BaseFunction {

    /**
     * 
     */
    private static final long serialVersionUID = -5036889873716017578L;
    transient SerDe serde;
    Fields fields;
    Properties configSerde = new Properties();

    
    public Deserialize(List<Expr> exprs) {
        super();
        this.configSerde =  Serde.getSerdeProperties(exprs);      
    }
    
    @Override
    public void prepare(Map conf, TridentOperationContext context) {

        super.prepare(conf, context);
        serde = Serde.getSerde(configSerde);
        
        MessageParserPrims messageStructure = new MessageParserPrims(serde, null);
        
        this.fields = new Fields(messageStructure.getFields());
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
         
      try {
          Values values = new Values();
          Object result = serde.deserialize(new Text(tuple.getString(0)));

          MessageParserPrims topologyBuilder = new MessageParserPrims(serde, result);
          
          for(String field:fields){
              values.add(topologyBuilder.getFieldData(field.toString()));
          }

          collector.emit(values);

      } catch (SerDeException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
    }
}