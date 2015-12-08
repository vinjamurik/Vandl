package mil.nga.xid.vandl.dataflow.controllers;

import mil.nga.xid.vandl.dataflow.utils.Configurator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public final class KafkaService {
    private static Properties producer, consumer;

    private KafkaService(){};

    public static void init() throws IOException{
        producer = Configurator.readPropertiesFromStream(KafkaService.class.getClassLoader().getResourceAsStream(Configurator.getProperty("kafkaProducerLocation").toString()));
        consumer = Configurator.readPropertiesFromStream(KafkaService.class.getClassLoader().getResourceAsStream(Configurator.getProperty("kafkaConsumerLocation").toString()));
    }

    private static Producer<String,String> createProducer(Properties p){
        return new KafkaProducer<String,String>(p == null ? producer : p);
    }

    public static Producer<String,String> createProducer(){
        return createProducer(null);
    }

    private static Consumer<String,String> createConsumer(Properties p){
        return new KafkaConsumer<String,String>(p == null ? consumer : p);
    }

    public static Consumer<String,String> createConsumer(){
        return createConsumer(null);
    }

    public static void sendMessage(Producer<String, String> pArg,String topic, String message){
        Producer<String,String> p = pArg == null ? new KafkaProducer<String,String>(producer) : pArg;
        p.send(new ProducerRecord<String,String>(topic,Long.toString(System.nanoTime()),message));
    }

    public static void sendMessage(String topic, String message){
        sendMessage(null,topic,message);
    }

    public static ConsumerRecords<String,String> pollTopics(int time, String... topic) throws IOException{
        Consumer<String,String> c = new KafkaConsumer<String,String>(consumer);
        c.subscribe(Arrays.asList(topic));
        ConsumerRecords<String,String> recs = c.poll(time);
        c.close();
        return recs;
    }

    public static Integer countTopics(String... topics) throws IOException{
        return pollTopics(Integer.parseInt(Configurator.getProperty("kafkaDefaultPollTime").toString()),topics).count();
    }
}
