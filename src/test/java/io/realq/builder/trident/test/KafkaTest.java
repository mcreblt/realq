package io.realq.builder.trident.test;

import io.realq.builder.trident.test.utils.FieldTestDef;
import io.realq.builder.trident.test.utils.Split;
import io.realq.builder.trident.test.utils.TestHelper;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Expr.Category;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import com.google.common.cache.CacheLoader;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import kafka.utils.ZKStringSerializer$;

public class KafkaTest {

    @Test
    public void testKafkaSource() {

        String inputString = 
                "CREATE SOURCE users(user_id INT, details STRUCT<name:STRING,gender:STRING,bool:BOOLEAN>)" 
                + "SRCCLASS 'io.realq.builder.trident.source.KafkaSource' "
                + "SRCPROPERTIES('zkHosts' = '0.0.0.0:2000', "
                + "              'topic' = 'test-topic', "
                + "              'spoutType' = 'OpaqueTridentKafkaSpout' "
                + "); "
                + "CREATE STREAM test " 
                + "SELECT user_id AS uid, details.gender AS dg FROM users;"
                ;
        
        List<FieldTestDef> fields = new ArrayList<FieldTestDef>();
        
        fields.add(new FieldTestDef("uid", "1;2;", "dg", "[[INTEGER:1,STRING:male],[INTEGER:2,STRING:female]]"));

        KeyedMessage<Integer, byte[]> data1 = new KeyedMessage<>("test-topic", "{\"user_id\":1, \"details\":{\"name\": \"Matt\",\"gender\": \"male\",\"bool\": true}}"
                .getBytes(StandardCharsets.UTF_8));
        
        KeyedMessage<Integer, byte[]> data2 = new KeyedMessage<>("test-topic","{\"user_id\":2, \"details\":{\"name\": \"test\",\"gender\": \"female\",\"bool\": false}}"
                .getBytes(StandardCharsets.UTF_8));

        @SuppressWarnings("rawtypes")
        List<KeyedMessage> mocks = new ArrayList<KeyedMessage>();
        mocks.add(data1);
        mocks.add(data2);
        
        TestHelper.executeKafkaConsumerQueryTest(inputString, fields, mocks);
        
    }
    
    @Test
    public void testKafkaSink() {
        int kafkaPort = TestUtils.choosePort();
        String inputString = 
                  "CREATE SOURCE users(user_id INT, details STRUCT<name:STRING,gender:STRING,bool:BOOLEAN>)"
                + "SRCCLASS 'io.realq.builder.trident.source.FixedBatchSource' "
                + "SRCPROPERTIES('valuesSeperator' = ';', "
                + "              'values' = '{\"user_id\":1, \"details\":{\"name\": \"Matt\",\"gender\": \"male\",\"bool\": true}};{\"user_id\":2, \"details\":{\"name\": \"test\",\"gender\": \"female\",\"bool\": false}}', "
                + "              'batchSize' = '3', "
                + "              'cycle' = 'false' "
                + "); "
                
                + "CREATE SINK UsersOut " 
                + "SINKCLASS 'io.realq.builder.trident.sink.KafkaSink' "
                + "SINKPROPERTIES('metadata.broker.list' = 'localhost:"+kafkaPort+"', "
                + "               'request.required.acks' = '1', "
                + "               'topic' = 'test' ) "
                + "SELECT user_id, details.gender FROM users;";
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("{\"user_id\":1,\"details.gender\":\"male\"}");
        fields.add("{\"user_id\":2,\"details.gender\":\"female\"}");
        
        TestHelper.executeKafkaProducerQueryTest(inputString, fields, kafkaPort);
        
    }
}