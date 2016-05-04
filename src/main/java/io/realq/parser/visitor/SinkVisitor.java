package io.realq.parser.visitor;

import io.realq.parser.ParserHelper;
import io.realq.parser.element.ElementSink;
import io.realq.parser.element.ElementsGraph;
import io.realq.parser.expr.Expr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.realq.query.antlr4.QueryBaseVisitor;
import io.realq.query.antlr4.QueryParser;
import io.realq.query.antlr4.QueryParser.CreateSinkContext;
import io.realq.query.antlr4.QueryParser.PropertiesContext;
import io.realq.query.antlr4.QueryParser.SelectExprContext;
import io.realq.query.antlr4.QueryParser.SinkContext;
import io.realq.query.antlr4.QueryParser.TableNameContext;

public class SinkVisitor extends QueryBaseVisitor<Void> {
    final Logger logger = LoggerFactory.getLogger(SinkVisitor.class);
    ElementsGraph graph;

    public SinkVisitor(ElementsGraph graph) {
        super();
        this.graph = graph;
    }

    @Override
    public Void visitSink(SinkContext ctx) {

        CreateSinkContext createContext = (QueryParser.CreateSinkContext) ctx.parent;
        TableNameContext table = createContext.tableName();

        logger.debug("Visiting sink SinkClass:" + ctx.sinkClass().className().getText() + "; table:" + table.getText());

        ElementSink sink = new ElementSink();

        sink.setSinkClass(ParserHelper.stripQuotes(ctx.sinkClass().className().getText()));
        
        for (PropertiesContext property : createContext.sink().sinkClass().properties()) {
            sink.setSinkProperty(ParserHelper.stripQuotes(property.key().getText()),
                    ParserHelper.stripQuotes(property.value().getText()));
        }

        for(SelectExprContext expr: createContext.select().selectExpr()){
            String outputField = ParserHelper.alias(expr);
            
            sink.addInput(new Expr(outputField, Expr.Category.FIELD));

        }
        graph.addSourceName(createContext.select().tableName().getText());
        graph.addVertex(table.getText(), sink);
        
        return super.visitSink(ctx);
    }
    
}
