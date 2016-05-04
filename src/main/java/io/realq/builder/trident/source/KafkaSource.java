package io.realq.builder.trident.source;

import java.util.Properties;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.spout.SchemeAsMultiScheme;


public class KafkaSource implements Source{
    public Stream getSource(String streamId, TridentTopology topology, Properties properties) {

        String zkHosts = properties.getProperty("zkHosts");
        String topic = properties.getProperty("topic"); 
        String spoutType = properties.getProperty("spoutType"); 
        
        
        BrokerHosts zk = new ZkHosts(zkHosts);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic);

        spoutConf.scheme = new SchemeAsMultiScheme(new NamedStringScheme(streamId));
        
        Stream stream;
        switch (spoutType) {
        case "OpaqueTridentKafkaSpout":
            OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
            stream = topology.newStream(streamId, spout);
            break;
        default:
            stream = null;
            break;
        }
        
        return stream;
    }
}
