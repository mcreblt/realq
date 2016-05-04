package io.realq.parser;

import io.realq.parser.element.ElementsGraph;

import java.util.List;

public interface GraphParser {
	<T> List<ElementsGraph> parse(String sql);
}
