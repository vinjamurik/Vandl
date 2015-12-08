package mil.nga.xid.vandl.dataflow;

import mil.nga.xid.vandl.dataflow.controllers.CSVService;
import mil.nga.xid.vandl.dataflow.controllers.KafkaService;
import mil.nga.xid.vandl.dataflow.utils.Configurator;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.nio.file.*;

public final class Driver {
    public static void main(String[] args) throws IOException,IndexOutOfBoundsException,InterruptedException{
        Configurator.init();
        KafkaService.init();
        Path dir = Paths.get(args[0]), absDir = dir.toAbsolutePath();
        WatchService watcher = dir.getFileSystem().newWatchService();
        dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
        WatchKey watchKey = watcher.take();
        while(true) {
            for (WatchEvent e : watchKey.pollEvents()) {
                Producer<String, String> producer = KafkaService.createProducer();
                CSVParser parser = CSVService.readFromFile(absDir.toString()+"/"+e.context().toString());//local file location
                for(CSVRecord r : parser){
                    KafkaService.sendMessage(producer,args[1],r.toMap().toString());//topic name
                }
                parser.close();
                producer.close();
            }
        }
        //System.out.println(KafkaService.pollTopics(1000,args[0]).count());
    }
}
