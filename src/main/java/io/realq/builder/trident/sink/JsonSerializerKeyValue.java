package io.realq.builder.trident.sink;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Expr.Category;
import io.realq.parser.expr.Types;

import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import backtype.storm.tuple.Values;

public class JsonSerializerKeyValue extends BaseFunction {

    private List<Expr> args;

    public JsonSerializerKeyValue(List<Expr> args) {
        super();
        this.args = args;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Values values = new Values();
        
        values.add(args.get(0).getStringValue());
        
        JsonObject value = new JsonObject();
        for (Expr expr : args) {
            if (expr.category.equals(Category.FIELD)) {
                Primitive primitive = (Primitive) tuple.getValueByField(expr.value.toString());
                value.add(expr.value.toString(), getJsonPrimitive(primitive));
            }
        }
        values.add(value.toString());

        collector.emit(values);
    }
    private JsonElement getJsonPrimitive(Primitive primitive){
        if (Types.isNumeric(primitive.getType())) {
            return new JsonPrimitive(primitive.getNumber());
        } else if (primitive.getType().equals(Types.Type.BOOLEAN)) {
            return new JsonPrimitive(primitive.getBoolean());
        } else {
            return new JsonPrimitive(primitive.getString());
        }
    }
}
