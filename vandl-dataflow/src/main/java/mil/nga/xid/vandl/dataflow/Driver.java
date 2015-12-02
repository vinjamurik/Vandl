package mil.nga.xid.vandl.dataflow;

import mil.nga.xid.vandl.dataflow.controllers.CSVService;
import mil.nga.xid.vandl.dataflow.controllers.KafkaService;
import mil.nga.xid.vandl.dataflow.utils.Configurator;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;

public final class Driver {
    public static void main(String[] args) throws IOException,IndexOutOfBoundsException{
        Configurator.init();
        KafkaService.init();
        switch(args[0]){
            case "csvToDataLake":
                Producer<String, String> p = KafkaService.createProducer();
                for(CSVRecord r : CSVService.readFromFile(args[1])){
                    KafkaService.sendMessage(args[2],r.toMap().toString(),p);
                }
                p.close();
                break;
            default:
                break;
        }
    }
}
