package mil.nga.xid.vandl.webapp.dataflow;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class DataflowService {
    private Properties producer,consumer,dataflow;

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

    public void sendMessage(String topic, String message){
        Producer<String,String> p = new KafkaProducer<>(producer);
        p.send(new ProducerRecord<>(topic,message));
        p.close();
    }

    public ConsumerRecords<String,String> pollTopics(int time, String... topic){
        Consumer<String,String> c = new KafkaConsumer<>(consumer);
        c.subscribe(Arrays.asList(topic));
        ConsumerRecords<String,String> recs = c.poll(time);
        c.close();
        return recs;
    }

    public static Scanner readFromFile(String fileLocation) throws IOException {
        File f = new File(fileLocation);
        if(!f.exists()){
            throw new FileNotFoundException("File does not exist");
        }
        if(!f.canRead()){
            throw new IOException("READ permission denied on file");
        }
        return new Scanner(f);
    }
}
