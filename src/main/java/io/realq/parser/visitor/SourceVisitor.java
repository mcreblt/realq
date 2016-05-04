package io.realq.parser.visitor;

import io.realq.parser.ParserHelper;
import io.realq.parser.element.ElementFunction;
import io.realq.parser.element.ElementSource;
import io.realq.parser.element.ElementsGraph;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Expr.Category;
import io.realq.parser.expr.FieldDef;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.realq.query.antlr4.QueryBaseVisitor;
import io.realq.query.antlr4.QueryParser;
import io.realq.query.antlr4.QueryParser.CreateSourceContext;
import io.realq.query.antlr4.QueryParser.FieldDefContext;
import io.realq.query.antlr4.QueryParser.PropertiesContext;
import io.realq.query.antlr4.QueryParser.SourceContext;
import io.realq.query.antlr4.QueryParser.TableNameContext;
import io.realq.serde.Serde;

public class SourceVisitor extends QueryBaseVisitor<Void> {
    final Logger logger = LoggerFactory.getLogger(SourceVisitor.class);
    ElementsGraph graph;

    public SourceVisitor(ElementsGraph graph) {
        super();
        this.graph = graph;
    }

    @Override
    public Void visitSource(SourceContext ctx) {
        // source vertex
        // deserialization vertex

        CreateSourceContext createContext = (QueryParser.CreateSourceContext) ctx.parent;
        TableNameContext table = createContext.tableName();

        ElementSource source = new ElementSource();
        // "INPUT_"+table.getText()

        logger.debug("Visiting spout SourceClass:" + ctx.srcClass().className().getText() + "; table:" + table.getText());

        source.setSourceClass(ParserHelper.stripQuotes(ctx.srcClass().className().getText()));
        List<FieldDefContext> fieldsDef = createContext.fieldDef();
        for (FieldDefContext fieldDef : fieldsDef) {
            Expr output = new Expr(new FieldDef(fieldDef.type().getText(), fieldDef.anyName().getText()),
                    Category.FIELD_DEF);
            source.addFieldDef(output);
        }
        
        for (PropertiesContext property : createContext.source().srcClass().properties()) {
            source.setSourceProperty(ParserHelper.stripQuotes(property.key().getText()),
                    ParserHelper.stripQuotes(property.value().getText()));
        }

        graph.addVertex("RAW_"+table.getText(), source);

        // add deserialization function
        ElementFunction deserializationFunction = new ElementFunction();
        deserializationFunction.setName("DESERIALIZE");
        deserializationFunction.addInput(new Expr(table.getText(), Expr.Category.FIELD));
        
        for(Expr outputField: Serde.getOutputFields(source.getFieldDefs())){
            deserializationFunction.addOutput(outputField);
        }
        
        for(Expr outf: source.getFieldDefs()){
            deserializationFunction.addArg(outf);
        }
        
        graph.addVertex(table.getText(), deserializationFunction);
        
        graph.addEdge("RAW_"+table.getText(), table.getText());
        
        return super.visitSource(ctx);
    }
    
}
