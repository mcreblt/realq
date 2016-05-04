package io.realq.parser.visitor;

import java.util.ArrayList;

import io.realq.parser.ParserHelper;
import io.realq.parser.element.ElementAggregate;
import io.realq.parser.element.ElementFilter;
import io.realq.parser.element.ElementFunction;
import io.realq.parser.element.ElementsGraph;
import io.realq.parser.element.PlannerHelper;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;

import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.realq.query.antlr4.QueryBaseVisitor;
import io.realq.query.antlr4.QueryLexer;
import io.realq.query.antlr4.QueryParser.AggrWindowContext;
import io.realq.query.antlr4.QueryParser.AliasContext;
import io.realq.query.antlr4.QueryParser.ComparisonContext;
import io.realq.query.antlr4.QueryParser.ExprContext;
import io.realq.query.antlr4.QueryParser.FieldContext;
import io.realq.query.antlr4.QueryParser.FunctionContext;
import io.realq.query.antlr4.QueryParser.LiteralContext;
import io.realq.query.antlr4.QueryParser.LogicalContext;
import io.realq.query.antlr4.QueryParser.ParallelismContext;
import io.realq.query.antlr4.QueryParser.ParenthesesContext;
import io.realq.query.antlr4.QueryParser.SelectContext;
import io.realq.query.antlr4.QueryParser.SelectExprContext;
import io.realq.query.antlr4.QueryParser.WhereExprContext;

public class SelectVisitor extends QueryBaseVisitor<Void> {

    final Logger logger = LoggerFactory.getLogger(SelectVisitor.class);

    ElementsGraph graph;

    String sourceStreamName;

    public SelectVisitor(ElementsGraph graph) {
        super();
        this.graph = graph;
    }

    @Override
    public Void visitSelect(SelectContext ctx) {
        sourceStreamName = ctx.tableName().getText();
        graph.addSourceName(sourceStreamName);
        logger.debug("Visiting select table:" + ctx.tableName().getText());
        graph.addVertex(sourceStreamName);
        //Vertex to order elements coming after filtered stream
        graph.addVertex("FILTERED");
        graph.addEdge(sourceStreamName, "FILTERED");
        return super.visitSelect(ctx);
    }

    @Override
    public Void visitParallelism(ParallelismContext ctx) {
        logger.debug("Visiting visitParallelism - value:" + ctx.getText());
        graph.setParallelism(Integer.parseInt(ctx.getText()));
        return super.visitParallelism(ctx);
    }

    @Override
    public Void visitWhereExpr(WhereExprContext ctx) {
        String inputField = ctx.getText();

        logger.debug("Visiting visitWhereExpr - input:" + inputField);

        Expr input = new Expr(inputField, Expr.Category.FIELD);

        ElementFilter element = new ElementFilter();
        element.setName("FILTER");
        element.addInput(input);
        element.addArg(input);

        graph.addVertex("WHERE_" + input, element);
        graph.addEdge(input.getStringValue(), "WHERE_" + input);
        graph.addEdge("WHERE_" + input, "FILTERED");

        return super.visitWhereExpr(ctx);
    }

    @Override
    public Void visitAlias(AliasContext ctx) {

        SelectExprContext parentCtx = (SelectExprContext) ctx.parent;

        String inputField = parentCtx.expr().getText();
        String outputField = ParserHelper.alias(parentCtx);
        logger.debug("Visiting alias - input:" + inputField + "; output:"
                + outputField);

        Expr input = new Expr(inputField, Expr.Category.FIELD);
        Expr output = new Expr(outputField, Expr.Category.FIELD);

        ElementFunction element = new ElementFunction();
        element.setName("COPY");
        element.addInput(input);
        element.addOutput(output);
        // no joins yet
        String node = outputField;
        // parent is table name
        String parent = input.getStringValue();

        graph.addVertex(node, element);
        graph.addEdge(parent, node);
        return super.visitAlias(ctx);
    }

    @Override
    public Void visitFunction(FunctionContext ctx) {
        // parent.parent -> function | condition

        ArrayList<Expr> inputNames = new ArrayList<Expr>();
        String inputName = "";
        for (ExprContext expr : ctx.expr()) {
            inputName += expr.getText();
            logger.debug("Visiting function - inputs:" + expr.getText() );
            inputNames.add(new Expr(expr.getText(), Expr.Category.FIELD));
        }
        
        Expr input = new Expr(inputName, Expr.Category.FIELD);
        Expr output = new Expr(ctx.getText(), Expr.Category.FIELD);

        String parent = inputName;
        String node = ctx.getText();

        if (null != ctx.anyName()) {
            logger.debug("Visiting function - parent:" + inputName + "; name:"
                    + ctx.anyName().getText());
            ElementFunction element = new ElementFunction();
            element.setName(ctx.anyName().getText());
            for(Expr in:inputNames){
                element.addInput(in);
                element.addArg(in);    
            }
            
            element.addOutput(output);

            graph.addVertex(node, element);
            
            for(Expr in:inputNames){
                graph.addEdge(in.getStringValue(), node);  
            }
            
        } else {
            logger.debug("Visiting aggregate - :" + inputName + "; name:"
                    + ctx.aggrName().getText());

            ElementAggregate element = new ElementAggregate();
            element.setName(ctx.aggrName().getText());
            element.addInput(input);
            element.addArg(input);
            element.addOutput(output);

            // element.addGroupBy(e);
            // only top level aggr allowed
            SelectContext selectCtx = (SelectContext) ctx.parent.parent;
            if (null != selectCtx.groupExpr().aggrWindow()) {
                AggrWindowContext aggrWindow = selectCtx.groupExpr()
                        .aggrWindow();
                String paramsString = aggrWindow.functionParams().INT_PARAMS()
                        .getText();
                String[] params = paramsString.split(",");
                element.setWindowHop(Long.parseLong(params[0].trim()));
                element.setWindowSize(Long.parseLong(params[1].trim()));

            }
            for (ExprContext expr : selectCtx.groupExpr().expr()) {
                Expr groupBy = new Expr(expr.getText(), Expr.Category.FIELD);
                element.addGroupBy(groupBy);
            }

            graph.addVertex(node, element);
            graph.addEdge(parent, node);
            // Aggregations come after filtered stream
            graph.addEdge("FILTERED", node);

        }

        Void result = super.visitFunction(ctx);
        return result;
    }

    @Override
    public Void visitParentheses(ParenthesesContext ctx) {

        String inputField = ctx.getText().substring(1,
                ctx.getText().length() - 1);
        String outputField = ctx.getText();
        logger.debug("Visiting Parentheses - input:" + inputField + "; output:"
                + outputField);

        Expr input = new Expr(inputField, Expr.Category.FIELD);
        Expr output = new Expr(outputField, Expr.Category.FIELD);

        ElementFunction element = new ElementFunction();
        element.setName("COPY");
        element.addInput(input);
        element.addOutput(output);

        graph.addVertex(outputField, element);
        graph.addEdge(inputField, outputField);

        return super.visitParentheses(ctx);
    }

    @Override
    public Void visitComparison(ComparisonContext ctx) {

        RuleContext parentCtx = ctx.parent;
        Expr output = new Expr(ctx.getText(), Expr.Category.FIELD);

        String node = ctx.getText();
        parentCtx.getChild(1);
        String parentLeft = ctx.getChild(0).getText();

        logger.debug("Visiting comparison - operator:"
                + ctx.getChild(1).getChild(0));
        TerminalNode oper = (TerminalNode) ctx.getChild(1).getChild(0);
        int operatorIndex = oper.getSymbol().getType();
        Expr operator = new Expr(QueryLexer.ruleNames[operatorIndex - 1],
                Expr.Category.OPERATOR);

        String parentRight = ctx.getChild(2).getText();
        Expr inputLeft = new Expr(parentLeft, Expr.Category.FIELD);

        Expr inputRight = new Expr(parentRight, Expr.Category.FIELD);

        logger.debug("Visiting comparison - parentLeft:" + parentLeft
                + "; parentRight:" + parentRight + "; operator:" + operator
                + "; name:" + ctx.getText() + "; output:" + output);

        ElementFunction element = new ElementFunction();
        element.setName("COMPARISON");
        element.addInput(inputLeft);
        element.addInput(inputRight);
        element.addArg(inputLeft);
        element.addArg(operator);
        element.addArg(inputRight);

        element.addOutput(output);

        graph.addVertex(node, element);
        graph.addEdge(parentLeft, node);
        graph.addEdge(parentRight, node);

        return super.visitComparison(ctx);
    }

    @Override
    public Void visitLogical(LogicalContext ctx) {

        String parentLeft = ctx.getChild(0).getText();
        Expr inputLeft = new Expr(parentLeft, Expr.Category.FIELD);

        String parentRight = ctx.getChild(2).getText();
        Expr inputRight = new Expr(parentRight, Expr.Category.FIELD);

        Expr output = new Expr(ctx.getText(), Expr.Category.FIELD);

        Expr operator = new Expr(ctx.getChild(1).getText().toUpperCase(),
                Expr.Category.OPERATOR_LOGICAL);

        logger.debug("Visiting visitLogical: parentLeft" + parentLeft
                + "; parentRight:" + parentRight + "; operator:" + operator
                + "; output:" + output.getStringValue());
        ElementFunction element = new ElementFunction();
        element.setName("LOGICAL");
        element.addInput(inputLeft);
        element.addInput(inputRight);
        element.addArg(inputLeft);
        element.addArg(operator);
        element.addArg(inputRight);
        element.addOutput(output);

        String node = ctx.getText();
        graph.addVertex(node, element);
        graph.addEdge(parentLeft, node);
        graph.addEdge(parentRight, node);
        Void result = super.visitLogical(ctx);

        return result;
    }

    @Override
    public Void visitField(FieldContext ctx) {
        logger.debug("Visiting field - primitive:" + ctx.getText());
        graph.addVertex(ctx.getText());
        graph.addEdge(this.sourceStreamName, ctx.getText());

        return super.visitField(ctx);
    }

    @Override
    public Void visitLiteral(LiteralContext ctx) {

        Expr literal = null;
        Primitive primitive = null;
        String value = ctx.getText();
        if (null != ctx.STRING_LITERAL()) {
            primitive = new Primitive(PlannerHelper.stripQuotes(ctx
                    .getText()));
        } else if (null != ctx.NUMERIC_LITERAL()) {
            primitive = new Primitive(PlannerHelper.parseNumeric(ctx
                    .getText()));
        } else if (null != ctx.BOOLEAN_LITERAL()) {
            primitive = new Primitive(Boolean.parseBoolean(ctx
                    .getText()));
        } else if (null != ctx.INT_LITERAL()) {
            primitive = new Primitive(PlannerHelper.parseNumeric(ctx
                    .getText()));
        }

        literal = new Expr(primitive, Expr.Category.LITERAL);
        Expr output = new Expr(ctx.getText(), Expr.Category.FIELD);
        ElementFunction element = new ElementFunction();
        element.setName("ADD");
        element.addOutput(output);
        element.addArg(literal);

        graph.addVertex(value, element);
        graph.addEdge(this.sourceStreamName, value);

        logger.debug("Visiting literal - primitive:" + primitive + "; output:"
                + literal + "; parent:" + sourceStreamName);

        return super.visitLiteral(ctx);
    }

}
