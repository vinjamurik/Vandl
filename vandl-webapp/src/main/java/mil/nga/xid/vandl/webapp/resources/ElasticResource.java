package mil.nga.xid.vandl.webapp.resources;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@Controller
@RequestMapping(value = "/elastic", produces = MediaType.APPLICATION_JSON_VALUE)
public final class ElasticResource {
    @Value("#{restTemplate}")
    private RestTemplate restTemplate;
    @Value("#{webappProperties.elasticBatchSize}")
    private Integer batchSize;
    @Value("#{webappProperties.elasticScrollSize}")
    private Integer scrollSize;
    @Value("#{webappProperties.elasticRandomQuery}")
    private String randomQuery;
    @Value("#{webappProperties.elasticAddress}")
    private String elasticAddress;

    private StringBuilder builder(){
        return new StringBuilder(elasticAddress);
    }

    @RequestMapping(method = RequestMethod.GET)
    public @ResponseBody String getIndices(){
        return restTemplate.getForObject(builder().append("_aliases").toString(),String.class);
    }

    @RequestMapping(path="/{index}",method = RequestMethod.GET)
    public @ResponseBody String getTypes(@PathVariable String index){
        return restTemplate.getForObject(builder().append(index+"/_mappings").toString(),String.class);
    }

    @RequestMapping(path = "/{index}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    public void delete(@PathVariable String index){
        restTemplate.delete(builder().append(index).toString());
    }

    @RequestMapping(path = "/{index}/{type}", method = RequestMethod.GET)
    public @ResponseBody String previewData(@PathVariable String index, @PathVariable String type){
        return restTemplate.getForObject(builder().append(index+"/"+type+"/_search").toString(),String.class);
    }

    @RequestMapping(path="/{index}/{type}/{from}/{finish}/{random}",method = RequestMethod.GET)
    public @ResponseBody String getData(@PathVariable String index, @PathVariable String type, @PathVariable Integer from, @PathVariable Integer finish, @PathVariable Boolean random){
        return restTemplate.postForObject(builder().append(index+"/"+type+"/_search").append("?from="+from).append("&size="+Math.min(batchSize,finish - from)).toString(),random ? randomQuery : "{}",String.class);
    }

    @RequestMapping(path="/{index}/{type}/{scrollId}",method=RequestMethod.GET)
    public @ResponseBody String getData(@PathVariable String index, @PathVariable String type, @PathVariable String scrollId){
        StringBuilder builder = scrollId.equals("-") ? builder().append(index+"/"+type+"/_search").append("?size="+scrollSize).append("&search_type=scan") :
                builder().append("_search/scroll").append("?scroll_id="+scrollId);
        return restTemplate.getForObject(builder.append("&scroll=1m").toString(),String.class);
    }
}
