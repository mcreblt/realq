package io.realq.parser.visitor;

import java.util.List;

import io.realq.parser.element.Element;
import io.realq.parser.element.ElementsGraph;
import io.realq.query.antlr4.QueryBaseVisitor;
import io.realq.query.antlr4.QueryParser.CreateSinkContext;
import io.realq.query.antlr4.QueryParser.CreateSourceContext;
import io.realq.query.antlr4.QueryParser.CreateStreamContext;
import io.realq.query.antlr4.QueryParser.SelectContext;

public class StmtsVisitor extends QueryBaseVisitor<Void> {

    List<ElementsGraph> graphs;

    public StmtsVisitor(List<ElementsGraph> graphs) {
        super();
        this.graphs = graphs;
    }

    @Override
    public Void visitCreateSource(CreateSourceContext ctx) {
        ElementsGraph graph = new ElementsGraph(ctx.tableName().getText());
        SourceVisitor visitor = new SourceVisitor(graph);
        graphs.add(graph);
        return visitor.visitCreateSource(ctx);
    }

    @Override
    public Void visitCreateSink(CreateSinkContext ctx) {
        ElementsGraph graph = new ElementsGraph(ctx.tableName().getText());
        SinkVisitor visitor = new SinkVisitor(graph);
        graphs.add(graph);
        return visitor.visitCreateSink(ctx);
    }
    
    // @Override
    // public Void visitSelect(SelectContext ctx) {
    // ElementsGraph graph = new ElementsGraph(ctx.getText());
    // // names from
    // SelectVisitor visitor = new SelectVisitor(graph);
    // graphs.add(graph);
    // return visitor.visitSelect(ctx);
    // }

    @Override
    public Void visitCreateStream(CreateStreamContext ctx) {
        ElementsGraph graph = new ElementsGraph(ctx.tableName().getText());
        graphs.add(graph);

        SelectVisitor visitor = new SelectVisitor(graph);
        visitor.visitSelect(ctx.select());
        return super.visitCreateStream(ctx);
    }
}
