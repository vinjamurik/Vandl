package mil.nga.xid.vandl.resources;

import mil.nga.xid.vandl.dataflow.DataflowService;
import org.springframework.beans.factory.annotation.Value;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/streaming")
public class KafkaResource {
    @Value("#{dataflowService}")
    private DataflowService dataflowService;

    @Path("/{topic}")
    @POST
    public Response postToTopic(@PathParam("topic") String topic, String json) throws IOException {
        dataflowService.sendMessage(topic,json);
        return Response.ok().build();
    }
}
