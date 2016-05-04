package io.realq.parser.expr;

public class FieldDef {
		
	String type;
	String name;
	
	public FieldDef(String type, String name) {
		super();
		this.type = type;
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}
	
}
