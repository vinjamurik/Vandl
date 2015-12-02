package mil.nga.xid.vandl.dataflow.controllers;

import mil.nga.xid.vandl.dataflow.utils.Configurator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public final class KafkaService {
    private static Properties producer, consumer;

    public static void init() throws IOException{
        producer = Configurator.readPropertiesFromStream(KafkaService.class.getClassLoader().getResourceAsStream(Configurator.getProperty("kafkaProducerLocation").toString()));
        consumer = Configurator.readPropertiesFromStream(KafkaService.class.getClassLoader().getResourceAsStream(Configurator.getProperty("kafkaConsumerLocation").toString()));
    }

    public static Producer<String,String> createProducer(Properties p){
        return new KafkaProducer<String, String>(p == null ? producer : p);
    }

    public static Producer<String,String> createProducer(){
        return createProducer(null);
    }

    public static void sendMessage(String topic, String message, Producer<String, String> pArg){
        Producer<String,String> p = pArg == null ? new KafkaProducer<String, String>(producer) : pArg;
        p.send(new ProducerRecord<String, String>(topic,Long.toString(System.nanoTime()),message));
    }

    public static void sendMessage(String topic, String message){
        sendMessage(topic,message,null);
    }

    public static ConsumerRecords<String,String> pollTopics(int time, String topic){
        Consumer<String,String> c = new KafkaConsumer<String,String>(consumer);
        c.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> rs = c.poll(time);
        c.close();
        return rs;
    }

    public static void printTopics(int time, String topic){
        for(ConsumerRecord<String,String> r : pollTopics(time,topic)){
            System.out.println(r.value());
        }
    }
}
