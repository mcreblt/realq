package io.realq.main;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import storm.trident.TridentTopology;
import io.realq.builder.trident.TridentTopologyBuilder;
import io.realq.parser.SqlGraphParser;
import io.realq.parser.element.ElementsGraph;

public class App {

    public static void main(String[] args) {
       
        String sql = "CREATE SOURCE users(user_id INT, details STRUCT<name:STRING,gender:STRING,bool:BOOLEAN>)"
                + "SRCCLASS 'io.realq.builder.trident.source.FixedBatchSource' "
                + "SRCPROPERTIES('valuesSeperator' = ';', "
                + "                'values' = '{\"user_id\":1, \"details\":{\"name\": \"Matt\",\"gender\": \"male\",\"bool\": true}};{\"user_id\":2, \"details\":{\"name\": \"test\",\"gender\": \"female\",\"bool\": false}}', "
                + "                'batchSize' = '5', "
                + "                'cycle' = 'true' "
                + "); "

                + "CREATE STREAM GenderCount " 
                + "  SELECT details.gender, COUNT(details.gender) AS usersByGender "
                + "    FROM users " 
                + "GROUP BY details.gender, WINDOW(3,3) " 
                + "PARALLELISM 5; "

                + "CREATE SINK UsersOut " 
                + "  SINKCLASS 'io.realq.builder.trident.sink.ConsoleSink' "
                + "  SELECT details.gender, usersByGender FROM GenderCount;"
                ;

        SqlGraphParser treeParser = new SqlGraphParser();
        List<ElementsGraph> graphs = treeParser.parse(sql);

        TridentTopology topology = new TridentTopology();
        TridentTopologyBuilder builder = new TridentTopologyBuilder(topology);

        builder.build(graphs);
        
        try {

            LocalCluster cluster = new LocalCluster();
            Config config = new Config();
            config.setDebug(true);

            cluster.submitTopology("test", config, topology.build());
            Thread.sleep(100000);

            cluster.shutdown();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
