package io.realq.builder.trident.source;

import storm.kafka.StringScheme;
import backtype.storm.tuple.Fields;

class NamedStringScheme extends StringScheme {

    private String output; 
    
    public NamedStringScheme(String output) {
        super();
        this.output = output;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(this.output);
    }
    
}