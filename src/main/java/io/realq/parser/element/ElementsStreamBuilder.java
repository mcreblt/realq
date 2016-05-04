package io.realq.parser.element;


public interface ElementsStreamBuilder {
  
    void buildStream(ElementSource element);
    void buildStream(ElementSink element);
    void buildStream(ElementFunction element);
    void buildStream(ElementFilter element);
    void buildStream(ElementAggregate element);
}