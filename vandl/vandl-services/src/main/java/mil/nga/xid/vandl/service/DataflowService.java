package mil.nga.xid.vandl.service;

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DataflowService {
    private Properties producer,consumer,dataflow;

    private AmazonS3Client amazonS3Client;

    public Properties getProducer() {
        return producer;
    }

    public void setProducer(Properties producer) {
        this.producer = producer;
    }

    public Properties getConsumer() {
        return consumer;
    }

    public void setConsumer(Properties consumer) {
        this.consumer = consumer;
    }

    public Properties getDataflow() {
        return dataflow;
    }

    public void setDataflow(Properties dataflow) {
        this.dataflow = dataflow;
    }

    public AmazonS3Client getAmazonS3Client() {
        return amazonS3Client;
    }

    public void setAmazonS3Client(AmazonS3Client amazonS3Client) {
        this.amazonS3Client = amazonS3Client;
    }

    public List<String> listTopics(){
        Consumer<String,String> c = new KafkaConsumer<>(consumer);
        List<String> topics = new ArrayList<>();
        for(Map.Entry<String,List<PartitionInfo>> e : c.listTopics().entrySet()){
            topics.add("\""+e.getKey()+"\"");
        }
        return topics;
    }

    public void sendMessage(String topic, String message){
        Producer<String,String> p = new KafkaProducer<>(producer);
        p.send(new ProducerRecord<>(topic,message));
        p.close();
    }

    public ConsumerRecords<String,String> pollTopics(int time, String groupId, String... topic){
        Properties consumer = new Properties();
        consumer.putAll(this.consumer);
        consumer.setProperty("group.id",groupId);
        Consumer<String,String> c = new KafkaConsumer<>(consumer);
        c.subscribe(Arrays.asList(topic));
        ConsumerRecords<String,String> recs = c.poll(time);
        c.close();
        return recs;
    }

    public Scanner readFromFile(String fileLocation) throws IOException {
        File f = new File(fileLocation);
        return new Scanner(f);
    }
}
