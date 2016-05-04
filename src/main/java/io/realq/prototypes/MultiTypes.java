package io.realq.prototypes;

import java.util.ArrayList;
import java.util.List;

import storm.trident.Stream;

public class MultiTypes {

    public static void main( String[] args )
    {
    	List<ElementBuilder> bbbb = new ArrayList<ElementBuilder>();
    	
    	ElementBuilderImpl aaa = new ElementBuilderImpl();
    	bbbb.add(aaa);
    	
    	for( ElementBuilder bbb:bbbb){
    		ElementStreamBuilderImpl sb = new ElementStreamBuilderImpl();
    		bbb.setStreamBuilder(sb);
//    		doStuff.stuff(bbb);
    	}
    }
    
}
