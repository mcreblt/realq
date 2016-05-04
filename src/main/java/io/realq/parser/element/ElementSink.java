package io.realq.parser.element;

import java.util.Properties;

public class ElementSink extends Element {

    private String sinkClass;
    private Properties sinkProperties = new Properties();
    
    public String getSinkClass() {
        return sinkClass;
    }
    public void setSinkClass(String sinkClass) {
        this.sinkClass = sinkClass;
    }

    public void setSinkProperty(String key, String value) {
        sinkProperties.setProperty(key, value);
    }

    public Properties getSinkProperties() {
        return sinkProperties;
    }
    
}
