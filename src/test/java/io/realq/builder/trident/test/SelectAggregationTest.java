package io.realq.builder.trident.test;

import io.realq.builder.trident.test.utils.FieldTestDef;
import io.realq.builder.trident.test.utils.TestHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class SelectAggregationTest {

    String mocks = 
            "1 Matt male;"
          + "2 joanna female;"
          + "3 chris male;"
          + "4 tom male;"
          + "5 jack male;"
          + "6 marry female;"
          + "7 judie female;"
          + "8 joanna female;"
          + "9 john male";
    
    List<String> mocksFields = Arrays.asList("user_id", "details.name", "details.gender");
    
    @Test
    public void testAggr() {
        
        String inputString = 
                "CREATE STREAM test " 
                  + "SELECT details.gender, "
                  +   "COUNT(user_id) AS test1 "
                  + "  FROM mocksStream "
                  + "GROUP BY details.gender";

        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("details.gender", "male;female;", "test1", "[[STRING:male,LONG:5],[STRING:female,LONG:4]]"));
          
        TestHelper.executeQueryTest(inputString, fields,  mocks, mocksFields);
    
    }
    
    @Test
    public void testAggrFiltered() {
        
        String inputString = 
                "CREATE STREAM test " 
                  + "SELECT details.gender, "
                  +   "COUNT(user_id) AS test1 "
                  + "  FROM mocksStream "
                  + " WHERE user_id > 3 "
                  + "GROUP BY details.gender";

        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("details.gender", "male;female;", "test1", "[[STRING:male,LONG:3],[STRING:female,LONG:3]]"));
          
        TestHelper.executeQueryTest(inputString, fields,  mocks, mocksFields);
    
    }
    @Test
    public void testAggrWindowed() {
        
        String inputString = 
                "CREATE STREAM test " 
                  + "SELECT details.gender, "
                  +   "COUNT(user_id) AS test1 "
                  + "  FROM mocksStream "
                  + "GROUP BY details.gender, WINDOW(3,3)";

        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("details.gender", "male;female;", "test1", "[[STRING:male,LONG:5],[STRING:female,LONG:4]]"));
          
        TestHelper.executeQueryTest(inputString, fields,  mocks, mocksFields, 4999);
    
    }
}