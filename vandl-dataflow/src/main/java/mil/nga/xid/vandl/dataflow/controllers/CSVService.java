package mil.nga.xid.vandl.dataflow.controllers;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import kafka.consumer.Consumer;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public final class CSVService {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("group.id", "something");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("zookeeper.connect","localhost:2181");
        props.put("auto.offset.reset", "smallest");
        ConsumerConnector conn = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String,Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("test",new Integer(1));
        for(MessageAndMetadata<byte[],byte[]> m : conn.createMessageStreams(topicMap).get("test").get(0)){
            System.out.println(new String(m.message()));
        }
    }

    public static CSVParser readFromFile(String fileLocation) throws IOException{
        return CSVFormat.DEFAULT.withHeader().parse(new FileReader(fileLocation));
    }

    public static CSVParser readFromFile(String fileLocation,String[] headers) throws IOException{
        return CSVFormat.DEFAULT.withHeader(headers).parse(new FileReader(fileLocation));
    }
}
