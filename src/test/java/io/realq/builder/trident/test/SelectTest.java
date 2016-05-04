package io.realq.builder.trident.test;

import io.realq.builder.trident.test.utils.FieldTestDef;
import io.realq.builder.trident.test.utils.TestHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class SelectTest {

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
    public void testSelectSimpleCaseConv() {
        
        String inputString = 
                "CREATE STREAM test " 
                  + "SELECT user_id, "
                  + "       UPPER(details.name) AS test1, "
                  + "       LOWER(UPPER(details.name)) AS test2, "
                  + "       upper(LOWER(UPPER(details.gender))) AS test3 "
                  + "  FROM mocksStream";
        

        
        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("user_id", "1;2;", "test1", "[[INTEGER:1,STRING:MATT],[INTEGER:2,STRING:JOANNA]]"));
        fields.add(new FieldTestDef("user_id", "3;4;", "test1", "[[INTEGER:3,STRING:CHRIS],[INTEGER:4,STRING:TOM]]"));
        fields.add(new FieldTestDef("user_id", "1;", "test2", "[[INTEGER:1,STRING:matt]]"));
        fields.add(new FieldTestDef("user_id", "1;", "test3", "[[INTEGER:1,STRING:MALE]]"));

        TestHelper.executeQueryTest(inputString, fields,  mocks, mocksFields);
    }
    
    @Test
    public void testSelectCondition() {
        
        String inputString = 
                "CREATE STREAM test " 
                  + "SELECT user_id, "
                  +   "IF(user_id=1, user_id, 'abc_string') AS test3, "
                  +   "IF(user_id=3, user_id, 'abc_string') AS test4 "
                  + "  FROM mocksStream";

        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("user_id", "1;2;3;", "test3", "[[INTEGER:1,INTEGER:1],[INTEGER:2,STRING:abc_string],[INTEGER:3,STRING:abc_string]]"));
        fields.add(new FieldTestDef("user_id", "1;2;3;", "test4", "[[INTEGER:1,STRING:abc_string],[INTEGER:2,STRING:abc_string],[INTEGER:3,INTEGER:3]]"));
        
        TestHelper.executeQueryTest(inputString, fields,  mocks, mocksFields);
    
    }
}