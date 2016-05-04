package io.realq.parser.expr;

public class OperatorLogical {
	
	static public enum Type {
		AND,
		OR,
		PLUS
	}
	
	Type value;

	public OperatorLogical(Type value) {
		super();
		this.value = value;
	}
	
	
}
