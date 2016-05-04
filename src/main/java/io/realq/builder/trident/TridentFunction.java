package io.realq.builder.trident;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.operation.Function;
import io.realq.parser.element.ElementFunction;


public class TridentFunction {
    final static Logger logger = LoggerFactory.getLogger(TridentFunction.class);

    Stream stream;

    public TridentFunction(Stream stream) {
        super();
        this.stream = stream;
    }

    public Stream buildStream(ElementFunction function, int parallelism) {

        List<String> streamFields = this.stream.getOutputFields().toList();
        List<String> inputFields = TridentUtils.getFields(function.getInputs());
        List<String> newFields = TridentUtils.getFields(function.getOutputs());
        newFields.removeAll(streamFields);
        logger.debug("Stream fields:" + streamFields.toString() + "; Stream:" + stream);
        logger.debug("Building Trident function:" + function.getOutputs());
        
        Function tridentFunction = TridentUtils.getFuntion(function.getName(), function.getArgs());
        
        if (function.getInputs() == null) {
            stream = stream.each(tridentFunction, new Fields(newFields));
        } else {
            stream = stream.each(new Fields(inputFields), tridentFunction, new Fields(newFields));
        }

        return stream;
    }

}
