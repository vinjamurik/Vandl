package mil.nga.xid.vandl.controllers;

import mil.nga.xid.vandl.dataflow.controllers.KafkaService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;

@Path("/streaming")
public class KafkaResource {
    @Path("/{topic}/count")
    @GET
    public Integer getCount(@PathParam("topic") String topic) throws IOException{
        return KafkaService.countTopics(topic);
    }
}
