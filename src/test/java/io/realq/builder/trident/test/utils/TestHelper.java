package io.realq.builder.trident.test.utils;

import io.realq.builder.trident.TridentTopologyBuilder;
import io.realq.parser.SqlGraphParser;
import io.realq.parser.element.ElementsGraph;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Expr.Category;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.Testing;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

public class TestHelper {

    public static MkClusterParam getClusterParm() {

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }

    public static void executeKafkaConsumerQueryTest(String inputString, List<FieldTestDef> fields, List<KeyedMessage> mocks) {

        Testing.withLocalCluster(getClusterParm(), new TestJob() {

            @Override
            public void run(ILocalCluster cluster) throws Exception {
                // TODO Auto-generated method stub

                // System.out.println(cluster.getClusterInfo().metaDataMap);

                String topic = "test-topic";
                // setup Broker
                int port = TestUtils.choosePort();
                Properties props = TestUtils.createBrokerConfig(0, port, true);
                props.setProperty("zookeeper.connect", "0.0.0.0:2000");
                KafkaConfig config = new KafkaConfig(props);

                Time mock = new MockTime();
                KafkaServer kafkaServer = TestUtils.createServer(config, mock);
                String[] arguments = new String[] { "--topic", topic, "--partitions", "1", "--replication-factor", "1" };
                ZkClient zkClient = new ZkClient("0.0.0.0:2000", 30000, 30000, ZKStringSerializer$.MODULE$);
                zkClient.delete("/consumers/group0");
                TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(arguments));

                List<KafkaServer> servers = new ArrayList<KafkaServer>();
                servers.add(kafkaServer);
                TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic,
                        0, 5000);

                TridentTopology topology = new TridentTopology();

                // parse sql
                SqlGraphParser treeParser = new SqlGraphParser();
                List<ElementsGraph> graphs = treeParser.parse(inputString);
                HashMap<String, Stream> streams = new HashMap<String, Stream>();
                TridentTopologyBuilder builder = new TridentTopologyBuilder(topology, streams);

                builder.build(graphs);
                
                for (FieldTestDef field : fields) {
                    createTestState(field, streams, topology);
                }
                
                cluster.submitTopology("Topology", new Config(), topology.build());

                // setup producer
                Properties properties = TestUtils.getProducerConfig("localhost:" + port);
                ProducerConfig producerConfig = new ProducerConfig(properties);
                @SuppressWarnings("rawtypes")
                Producer producer = new Producer<>(producerConfig);
                producer.send(scala.collection.JavaConversions.asScalaBuffer(mocks));

                Thread.sleep(10000);
                
                for (FieldTestDef field : fields) {
                    String result = field.getState().execute("drcpInput" + field.hashCode(), field.getId());

                    Assert.assertEquals(field.getExpected(), result);
                }
                
                producer.close();
                kafkaServer.shutdown();
                
            }
        });
    }

    public static void executeQueryTest(String inputString, List<FieldTestDef> fields, String mocks,
            List<String> mocksFields) {
        executeQueryTest(inputString, fields, mocks, mocksFields, 0);
    }

    public static void executeQueryTest(String inputString, List<FieldTestDef> fields, String mocks,
            List<String> mocksFields, int sleep) {

        Testing.withLocalCluster(getClusterParm(), new TestJob() {

            TridentTopology topology = new TridentTopology();
            HashMap<String, Stream> streams = new HashMap<String, Stream>();

            @Override
            public void run(ILocalCluster cluster) throws Exception {

                FeederBatchSpout feederSpout = new FeederBatchSpout(Arrays.asList("args"));

                Stream stream = topology
                        .newStream("spout1", feederSpout)
                        .parallelismHint(10)
                        .each(new Fields("args"), new Split(Arrays.asList(new Expr("args", Category.FIELD))),
                                new Fields(mocksFields));

                SqlGraphParser treeParser = new SqlGraphParser();
                List<ElementsGraph> graphs = treeParser.parse(inputString);

                streams.put("mocksStream", stream);

                TridentTopologyBuilder builder = new TridentTopologyBuilder(topology, streams);

                builder.build(graphs);

                for (FieldTestDef field : fields) {
                    createTestState(field, streams, topology);
                }

                cluster.submitTopology("Topology", new Config(), topology.build());

                boolean notAlive = true;

                ClusterSummary cls = cluster.getClusterInfo();

                while (notAlive) {
                    if (cls.get_topologies().get(0).get_status().equals("ACTIVE")) {
                        notAlive = false;
                    }
                }

                Thread.sleep(1000 - (System.currentTimeMillis() % 1000));
                feederSpout.feed(new Values(Lists.newArrayList(mocks)));
                ;
                Thread.sleep(sleep);

                for (FieldTestDef field : fields) {
                    String result = field.getState().execute("drcpInput" + field.hashCode(), field.getId());

                    Assert.assertEquals(field.getExpected(), result);
                }
                cluster.killTopology("Topology");
            }

        });
        
    }
    
    private static void createTestState(FieldTestDef field, HashMap<String, Stream> streams, TridentTopology topology) {
        LocalDRPC drpc = new LocalDRPC();
        TridentState state = streams
                .get("test")
                .partitionBy(new Fields(field.getTestField()))
                .partitionPersist(new MemoryMapState.Factory(),
                        new Fields(field.getIdField(), field.getTestField()),
                        new StateUpdater(field.getIdField()));

        topology.newDRPCStream("drcpInput" + field.hashCode(), drpc)
                .each(new Fields("args"), new Split(Arrays.asList(new Expr("args", Category.FIELD))),
                        new Fields(field.getIdField()))
                .project(new Fields(field.getIdField()))
                .stateQuery(state, new Fields(field.getIdField()), new MapGet(),
                        new Fields(field.getTestField()));
        field.setState(drpc);
    }

    public static void executeKafkaProducerQueryTest(String inputString, List<String> expected, int port) {

        Testing.withLocalCluster(getClusterParm(), new TestJob() {

            @Override
            public void run(ILocalCluster cluster) throws Exception {
                // TODO Auto-generated method stub

                // System.out.println(cluster.getClusterInfo().metaDataMap);

                String topic = "test";
                // setup Broker

                Properties props = TestUtils.createBrokerConfig(0, port, true);
                props.setProperty("zookeeper.connect", "0.0.0.0:2000");
                KafkaConfig config = new KafkaConfig(props);

                Time mock = new MockTime();
                KafkaServer kafkaServer = TestUtils.createServer(config, mock);
                String[] arguments = new String[] { "--topic", topic, "--partitions", "1", "--replication-factor", "1" };
                ZkClient zkClient = new ZkClient("0.0.0.0:2000", 30000, 30000, ZKStringSerializer$.MODULE$);
                zkClient.delete("/consumers/group0");
                TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(arguments));

                TridentTopology topology = new TridentTopology();

                // parse sql
                SqlGraphParser treeParser = new SqlGraphParser();
                List<ElementsGraph> graphs = treeParser.parse(inputString);
                HashMap<String, Stream> streams = new HashMap<String, Stream>();
                TridentTopologyBuilder builder = new TridentTopologyBuilder(topology, streams);

                builder.build(graphs);
                
                cluster.submitTopology("Topology", new Config(), topology.build());

                List<String> result = new ArrayList<>();
                
                // starting consumer
                Properties consumerProperties = TestUtils.createConsumerProperties("0.0.0.0:2000", "group0", "consumer0", -1);
                ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
                
                Map<String, Integer> topicCount = new HashMap<>();
                topicCount.put(topic, 1);

                Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
                List<KafkaStream<byte[], byte[]>> streamsKafka = consumerStreams.get(topic);
                for (final KafkaStream stream : streamsKafka) {
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    
                    if (it.hasNext()) {
                        result.add(new String(it.next().message()));
                    }
                    if (it.hasNext()) {
                        result.add(new String(it.next().message()));
                        
                    }
                }

                Assert.assertEquals(result.get(0), expected.get(0));
                Assert.assertEquals(result.get(1), expected.get(1));
                
                // cleanup
                consumer.shutdown();
                kafkaServer.shutdown();
                
            }
        });
        
    }
}
