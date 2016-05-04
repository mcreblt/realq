package io.realq.parser.expr;

public class Operator {
	
	static public enum Type {
		LT(-1), 		// '<'
		LT_EQ(-1,0), 	// '<='
		GT(1), 			// '>'
		GT_EQ(0,1),  	// '>='
		EQ(0), 			// '=='
		NOT_EQ1(-1,1),  // '!='
		NOT_EQ2(-1,1);  // '<>'
		
	    private final int[] value;
	    Type(int... value) { this.value = value; }
	    public int[] getValue() { return value; }
		
	}
	
	Type value;

	public Operator(Type value) {
		super();
		this.value = value;
	}
	
	
}
