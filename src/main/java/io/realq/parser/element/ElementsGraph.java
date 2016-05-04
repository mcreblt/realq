package io.realq.parser.element;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElementsGraph {
    // new streamName:
    //                         STREAM
    //                           |   
    //                          WHERE
    //              /            |       \
    //          name       -  othername - name = literal
    //                      \        /
    //        f(name) = f(othername)
    //                     |    
    //                   SELECT
    //             name    -   name
    //              |            |
    //          f(name)      alias name
    //              |
    //         f(f(name)  
    //              |
    //        f(f(name) alias
    //                     |
    //                    AGGR
    //                     |
    //                  f(f(name)
    final static Logger logger = LoggerFactory.getLogger(ElementsGraph.class);
    private Map<String, ElementSource> verticesSource = new HashMap<String, ElementSource>();
    private Map<String, ElementSink> verticesSink = new HashMap<String, ElementSink>();
    private Map<String, ElementFunction> verticesFunction = new HashMap<String, ElementFunction>();
    private Map<String, ElementFilter> verticesFilter = new HashMap<String, ElementFilter>();
    private Map<String, ElementAggregate> verticesAggregate = new HashMap<String, ElementAggregate>();

    private ArrayList<List<String>> edges = new ArrayList<List<String>>();

    private String streamName;
    private List<String> sourceNames = new ArrayList<String>();
    private int parallelism = 1;
    private ElementsStreamBuilder streamBuilder;
    DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(
            DefaultEdge.class);

    public void setStreamBuilder(ElementsStreamBuilder streamBuilder) {
        this.streamBuilder = streamBuilder;
    }

    public ElementsGraph(String streamName) {
        super();
        this.streamName = streamName;
        logger.debug("Setting stream name: " + streamName );
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean addSourceName(String e) {
        return sourceNames.add(e);
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public List<String> getSourceNames() {
        return sourceNames;
    }

    public void addVertex(String name) {
        logger.debug("Adding vertex name: " + name + ";");

        dag.addVertex(name);
    }

    public void addVertex(String name, ElementSource element) {
        logger.debug("Adding vertex name: " + name + "; element:" + element);
        verticesSource.put(name, element);
        dag.addVertex(name);
    }
    
    public void addVertex(String name, ElementSink element) {
        logger.debug("Adding vertex name: " + name + "; element:" + element);
        verticesSink.put(name, element);
        dag.addVertex(name);
    }
    
    public void addVertex(String name, ElementFunction element) {
        logger.debug("Adding vertex name: " + name + "; element:" + element);
        verticesFunction.put(name, element);
        dag.addVertex(name);
    }

    public void addVertex(String name, ElementFilter element) {
        logger.debug("Adding vertex name: " + name + "; element:" + element);
        verticesFilter.put(name, element);
        dag.addVertex(name);
    }

    public void addVertex(String name, ElementAggregate element) {
        logger.debug("Adding vertex name: " + name + "; element:" + element);
        verticesAggregate.put(name, element);
        dag.addVertex(name);
    }

    public void addEdge(String source, String target) {
        List<String> edge = new ArrayList<String>();
        edge.add(source);
        edge.add(target);
        edges.add(edge);
        logger.debug("Adding edge, source:" + source + "; target:" + target);
    }

    public void build() {

        for (List<String> edge : edges) {
            logger.debug("edge: " + edge.get(0) + ";" + edge.get(1) + ";");

            dag.addEdge(edge.get(0), edge.get(1));
        }

        Iterator<String> iterator = dag.iterator();
        while (iterator.hasNext()) {
            String vertex = iterator.next();

            if (verticesSource.get(vertex) != null) {
                logger.debug(vertex + " element:" + verticesSource.get(vertex)
                        + " edgesOf: " + dag.incomingEdgesOf(vertex));
                streamBuilder.buildStream(verticesSource.get(vertex));
            }
            
            if (verticesSink.get(vertex) != null) {
                logger.debug(vertex + " element:" + verticesSink.get(vertex)
                        + " edgesOf: " + dag.incomingEdgesOf(vertex));
                streamBuilder.buildStream(verticesSink.get(vertex));
            }
            if (verticesFunction.get(vertex) != null) {
                logger.debug(vertex + " element:"
                        + verticesFunction.get(vertex) + " edgesOf: "
                        + dag.incomingEdgesOf(vertex));
                streamBuilder.buildStream(verticesFunction.get(vertex));
            }
            if (verticesFilter.get(vertex) != null) {
                logger.debug(vertex + " element:" + verticesFilter.get(vertex)
                        + " edgesOf: " + dag.incomingEdgesOf(vertex));
                streamBuilder.buildStream(verticesFilter.get(vertex));
            }
            if (verticesAggregate.get(vertex) != null) {
                logger.debug(vertex + " element:"
                        + verticesAggregate.get(vertex) + " edgesOf: "
                        + dag.incomingEdgesOf(vertex));
                streamBuilder.buildStream(verticesAggregate.get(vertex));
            }
        }
    }
}