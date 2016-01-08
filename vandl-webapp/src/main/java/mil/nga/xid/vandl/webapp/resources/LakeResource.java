package mil.nga.xid.vandl.webapp.resources;

import com.amazonaws.services.s3.model.Bucket;
import mil.nga.xid.vandl.dataflow.DataflowService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping(value = "/lake", produces = MediaType.APPLICATION_JSON_VALUE)
public class LakeResource {
    @Value("#{dataflowService}")
    private DataflowService dataflowService;

    @RequestMapping(path = "/buckets",method = RequestMethod.GET)
    public @ResponseBody String getBuckets(){
        List<String> responseList = new ArrayList<String>();
        for(Bucket b: dataflowService.getAmazonS3Client().listBuckets()){
            responseList.add(b.getName());
        }
        return responseList.toString();
    }
}
