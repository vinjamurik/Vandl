package mil.nga.xid.vandl.webapp.resources;

import mil.nga.xid.vandl.dataflow.DataflowService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping(value = "/streaming", produces = MediaType.APPLICATION_JSON_VALUE)
public final class KafkaResource {
    @Value("#{dataflowService}")
    private DataflowService dataflowService;

    private StringBuilder builder(){
        return new StringBuilder();
    }

    @RequestMapping(path = "/{topic}/{time}",method = RequestMethod.GET)
    public @ResponseBody String getItemsInTopic(@PathVariable String topic, @PathVariable Integer time){
        StringBuilder b = builder().append("[");
        for(ConsumerRecord<String,String> r : dataflowService.pollTopics(time,topic)){
            b.append("{\"message\":\"").append(r.value()).append("\"},");
        }
        return b.deleteCharAt(b.length()-1).append("]").toString();
    }

    @RequestMapping(path = "/{topic}",method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public void postItemToTopic(@PathVariable String topic, @RequestBody String message){
        dataflowService.sendMessage(topic,message);
    }
}
