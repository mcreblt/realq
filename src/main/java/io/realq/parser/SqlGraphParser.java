package io.realq.parser;


import io.realq.parser.element.ElementsGraph;
import io.realq.parser.visitor.StmtsVisitor;
import io.realq.query.antlr4.QueryLexer;
import io.realq.query.antlr4.QueryParser;
import io.realq.query.antlr4.QueryParser.StmtsContext;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class SqlGraphParser implements GraphParser {

	@Override
	public List<ElementsGraph> parse(String sql) {
        QueryLexer lexer = new QueryLexer(new ANTLRInputStream(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        QueryParser parser = new QueryParser(tokens);
      
        
        StmtsContext queryContext = parser.stmts();

        List<ElementsGraph> graphs = new ArrayList<ElementsGraph>();
        
        StmtsVisitor visitor = new StmtsVisitor(graphs);
        visitor.visit(queryContext);

		return graphs;
	}

}
