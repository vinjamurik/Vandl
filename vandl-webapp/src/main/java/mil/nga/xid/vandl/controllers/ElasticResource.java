package mil.nga.xid.vandl.controllers;

import mil.nga.xid.vandl.utils.Configurator;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;

@Path("/elastic")
public class ElasticResource {
    private static final String ELASTIC_URL = Configurator.getProperties().getProperty("elasticAddress");
    private static final Integer BATCH_SIZE = Integer.parseInt(Configurator.getProperties().getProperty("elasticBatchSize"));
    private static final Integer SCROLL_SIZE = Integer.parseInt(Configurator.getProperties().getProperty("elasticScrollSize"));

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIndices(){
        return Configurator.request(new StringBuilder(ELASTIC_URL).append("_aliases").toString()).buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTypes(@PathParam("index") String index){
        return Configurator.request(new StringBuilder(ELASTIC_URL).append(index).append("/_mappings").toString()).buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}")
    @DELETE
    public Response delete(@PathParam("index") String index){
        return delete(index,null);
    }

    @Path("/{index}/{type}")
    @DELETE
    public Response delete(@PathParam("index") String index, @PathParam("type") String type){
        return Configurator.request(new StringBuilder(ELASTIC_URL).append(index).append("/").append(type != null ? type : "").toString()).buildDelete().invoke().ok().build();
    }

    @Path("/{index}/{type}/preview")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String previewData(@PathParam("index") String index, @PathParam("type") String type){
        return Configurator.request(new StringBuilder(ELASTIC_URL).append(index).append("/").append(type).append("/_search").toString()).buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}/{type}/{from}/{finish}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getData(@PathParam("index") String index, @PathParam("type") String type, @PathParam("from") Integer from, @PathParam("finish") Integer finish){
        return Configurator.request(new StringBuilder(ELASTIC_URL).append(index).append("/").append(type).append("/_search")
                .append(Configurator.asQueryString(Arrays.asList("from="+from,"size="+Math.min(BATCH_SIZE,finish - from)))).toString()).buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}/{type}/{scrollId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getData(@PathParam("index") String index, @PathParam("type") String type, @PathParam("scrollId") String scrollId){
        StringBuilder b = new StringBuilder(ELASTIC_URL).append(index).append("/").append(type).append("/_search");
        b = scrollId.isEmpty() ? b.append(Configurator.asQueryString(Arrays.asList("scroll=1m","size="+SCROLL_SIZE,"search_type=scan"))) :
                b.append("/scroll").append(Configurator.asQueryString(Arrays.asList("scroll=1m","scroll_id="+scrollId)));
        return Configurator.request(b.toString()).buildGet().invoke().readEntity(String.class);
    }
}
