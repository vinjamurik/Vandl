package mil.nga.xid.vandl.webapp.controller;

import mil.nga.xid.vandl.service.DataflowService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Scanner;
import java.util.UUID;

@Controller
@RequestMapping(value = "/stream", produces = MediaType.APPLICATION_JSON_VALUE)
public final class StreamController {
    @Value("#{dataflowService}")
    private DataflowService dataflowService;

    private StringBuilder builder(){
        return new StringBuilder();
    }

    @RequestMapping(path = "/list",method = RequestMethod.GET)
    public @ResponseBody String getTopics(){
        return dataflowService.listTopics().toString();
    }

    @RequestMapping(path = "/{topic}/{time}",method = RequestMethod.GET)
    public @ResponseBody String getMessages(@PathVariable String topic, @PathVariable Integer time,@RequestParam("groupId") String groupId){
        if(groupId.isEmpty()){
            groupId = UUID.randomUUID().toString();
        }
        StringBuilder b = builder().append("[");
        for(ConsumerRecord<String,String> r : dataflowService.pollTopics(time,groupId,topic)){
            b.append("{\"message\":\"").append(r.value()).append("\"},");
        }
        return b.deleteCharAt(b.length()-1).append("]").toString();
    }

    @RequestMapping(path = "/{topic}",method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public void sendMessage(@PathVariable String topic, @RequestBody String message){
        dataflowService.sendMessage(topic,message);
    }

    @RequestMapping(path = "/{topic}/_bulk",method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public void sendBulkMessage(@PathVariable String topic, @RequestBody String message){
        Scanner s = new Scanner(message);
        while(s.hasNextLine()) {
           sendMessage(topic, s.nextLine());
        }
    }
}
