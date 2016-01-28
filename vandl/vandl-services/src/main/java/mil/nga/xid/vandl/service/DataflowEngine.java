package mil.nga.xid.vandl.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.*;
import java.util.Scanner;

public final class DataflowEngine {
    private static DataflowService dataflowService;

    public static void main(String[] args) throws IOException,InterruptedException{
        ApplicationContext ctx;
        try {
            ctx = new FileSystemXmlApplicationContext("resources/vandl-dataflow.xml");
        }catch(Exception e){
            System.out.println("Could not find external spring context, please provide one and try again");
            ctx = new ClassPathXmlApplicationContext("vandl-dataflow.xml");
        }
        dataflowService = ctx.getBean("dataflowService",DataflowService.class);

        switch(dataflowService.getDataflow().getProperty("source")){
            case "file":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "stream":
                        fileToStream(dataflowService.getDataflow().getProperty("path"),
                                dataflowService.getDataflow().getProperty("topic"));
                        break;
                    default:
                        break;
                }
                break;
            case "directory":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "stream":
                        directoryToStream(dataflowService.getDataflow().getProperty("path"),
                                dataflowService.getDataflow().getProperty("pattern"),
                                dataflowService.getDataflow().getProperty("topic"));
                        break;
                    default:
                        break;
                }
                break;
            case "stream":
                switch (dataflowService.getDataflow().getProperty("sink")){
                    case "file":
                        streamToFile(dataflowService.getDataflow().getProperty("path"),
                                Integer.parseInt(dataflowService.getDataflow().getProperty("time")),
                                dataflowService.getDataflow().getProperty("groupId"),
                                dataflowService.getDataflow().getProperty("topic"));
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
        Scanner s = dataflowService.readFromFile(path);//local file location
        while(s.hasNextLine()){
            dataflowService.sendMessage(topic,s.nextLine());//topic name
        }
        s.close();
    }

    private static void directoryToStream(String path,String pattern, String topic) throws IOException,InterruptedException{
        Path dir = Paths.get(path), absDir = dir.toAbsolutePath();
        WatchService watcher = dir.getFileSystem().newWatchService();
        dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
        WatchKey watchKey = watcher.take();
        while(true) {
            for (WatchEvent e : watchKey.pollEvents()) {
                String filename = e.context().toString();
                if(filename.matches(pattern)) {
                    fileToStream(absDir.toString() + "/" +filename, topic);
                }
            }
        }
    }

    private static void streamToFile(String path, Integer time, String groupId, String... topic) throws IOException{
        Writer fw = new FileWriter(path);
        for(ConsumerRecord r : dataflowService.pollTopics(time,groupId,topic)){
            fw.append(r.value().toString()+"\n");
        }
        fw.close();
    }
}