package mil.nga.xid.vandl.webapp.controller;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import mil.nga.xid.vandl.service.DataflowService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping(value = "/lake")
public class LakeController {
    @Value("#{dataflowService}")
    private DataflowService dataflowService;

    @RequestMapping(path = "/buckets",method = RequestMethod.GET)
    public @ResponseBody String getBuckets(){
        List<String> responseList = new ArrayList<String>();
        for(Bucket b: dataflowService.getAmazonS3Client().listBuckets()){
            responseList.add("\""+b.getName()+"\"");
        }
        return responseList.toString();
    }

    @RequestMapping(path = "/{bucket}/list",method = RequestMethod.GET)
    public @ResponseBody String getObjects(@PathVariable String bucket, @RequestParam("prefix") String prefix){
        List<String> responseList = new ArrayList<String>();
        for(S3ObjectSummary s: dataflowService.getAmazonS3Client().listObjects(bucket,prefix).getObjectSummaries()){
            responseList.add("\""+s.getKey()+"\"");
        }
        return responseList.toString();
    }

    @RequestMapping(path = "/create/{bucket}/{key}",method = RequestMethod.POST)
    public @ResponseBody String createBucket(@PathVariable String bucket, @PathVariable String key, @RequestParam("file") MultipartFile file) throws IOException{
        File f = new File(file.getOriginalFilename());
        FileOutputStream fs = new FileOutputStream(f);
        fs.write(file.getBytes());
        dataflowService.getAmazonS3Client().putObject(bucket,key,f);
        return "{}";
    }
}
