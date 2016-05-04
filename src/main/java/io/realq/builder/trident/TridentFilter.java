package io.realq.builder.trident;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.operation.Filter;
import io.realq.parser.element.ElementFilter;

public class TridentFilter {
    final static Logger logger = LoggerFactory.getLogger(TridentFilter.class);

    Stream stream;

    public TridentFilter(Stream stream) {
        super();
        this.stream = stream;
    }

    public Stream buildStream(ElementFilter filter, int parallelism) {

        List<String> streamFields = this.stream.getOutputFields().toList();
        List<String> inputFields = TridentUtils.getFields(filter.getInputs());

        logger.debug("Stream fields:" + streamFields.toString() + "; Stream:" + stream);
        logger.debug("Building Trident function:" + filter.getOutputs());
        
        Filter tridentFunction = TridentUtils.getFilter(filter.getName(), filter.getArgs());
        
        stream = stream.each(new Fields(inputFields), tridentFunction);
        return stream;
    }

}
