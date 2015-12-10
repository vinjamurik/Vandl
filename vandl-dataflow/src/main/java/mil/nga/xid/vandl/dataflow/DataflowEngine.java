package mil.nga.xid.vandl.dataflow;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.*;
import java.util.Scanner;

public final class DataflowEngine {
    private static DataflowService dataflowService;

    public static void main(String[] args) throws IOException,IndexOutOfBoundsException,InterruptedException{
        ApplicationContext ctx = new FileSystemXmlApplicationContext("resources/vandl-spring-context.xml");
        dataflowService = ctx.getBean("dataflowService",DataflowService.class);

        switch(dataflowService.getDataflow().getProperty("source")){
            case "file":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "stream":
                        fileToStream(dataflowService.getDataflow().getProperty("path"),dataflowService.getDataflow().getProperty("topic"));
                        break;
                    default:
                        break;
                }
                break;
            case "directory":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "stream":
                        directoryToStream(dataflowService.getDataflow().getProperty("path"),dataflowService.getDataflow().getProperty("topic"));
                        break;
                    default:
                        break;
                }
                break;
            case "stream":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "file":
                        streamToFile(dataflowService.getDataflow().getProperty("path"),dataflowService.getDataflow().getProperty("topic"));
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    private static void fileToStream(String path, String topic) throws IOException{
        Scanner s = DataflowService.readFromFile(path);//local file location
        while(s.hasNextLine()){
            dataflowService.sendMessage(topic,s.nextLine());//topic name
        }
        s.close();
    }

    public static void directoryToStream(String path,String topic) throws IOException,InterruptedException{
        Path dir = Paths.get(path), absDir = dir.toAbsolutePath();
        WatchService watcher = dir.getFileSystem().newWatchService();
        dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
        WatchKey watchKey = watcher.take();
        while(true) {
            for (WatchEvent e : watchKey.pollEvents()) {
                fileToStream(absDir.toString()+"/"+e.context().toString(),topic);
            }
        }
    }

    public static void streamToFile(String path, String... topic) throws IOException{
        Writer fw = new FileWriter(path);
        for(ConsumerRecord r : dataflowService.pollTopics(1000,topic)){
            fw.append(r.value().toString()+"\n");
        }
        fw.close();
    }
}
