package mil.nga.xid.vandl.controllers;

import mil.nga.xid.vandl.utils.Configurator;

import javax.ws.rs.*;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;

@Path("/elastic")
public final class ElasticResource {
    private static final String URL = Configurator.getProperties().getProperty("elasticAddress");
    private static final Integer BATCH_SIZE = Integer.parseInt(Configurator.getProperties().getProperty("elasticBatchSize"));
    private static final Integer SCROLL_SIZE = Integer.parseInt(Configurator.getProperties().getProperty("elasticScrollSize"));
    private static final String RANDOM_QUERY = Configurator.getProperties().getProperty("elasticRandomQuery");

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIndices(){
        return Configurator.request(URL+"_aliases").buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTypes(@PathParam("index") String index){
        return Configurator.request(URL+index+"/_mappings").buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}")
    @DELETE
    public Response delete(@PathParam("index") String index){
        return Configurator.request(URL+index).buildDelete().invoke();
    }

    @Path("/{index}/{type}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String previewData(@PathParam("index") String index, @PathParam("type") String type){
        return Configurator.request(URL+index+"/"+type+"/_search").buildGet().invoke().readEntity(String.class);
    }

    @Path("/{index}/{type}/{from}/{finish}/{random}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getData(@PathParam("index") String index, @PathParam("type") String type, @PathParam("from") Integer from, @PathParam("finish") Integer finish, @PathParam("random") Boolean random){
        return Configurator.request(URL+index+"/"+type+"/_search"+Configurator.asQueryString(Arrays.asList("from="+from,"size="+Math.min(BATCH_SIZE,finish - from))))
                .buildPost(Entity.json(random ? RANDOM_QUERY : "{}" )).invoke().readEntity(String.class);
    }

    @Path("/{index}/{type}/{scrollId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getData(@PathParam("index") String index, @PathParam("type") String type, @PathParam("scrollId") String scrollId){
        String url = URL + ( scrollId.equals("-") ? index+"/"+type+"/_search"+Configurator.asQueryString(Arrays.asList("scroll=1m","size="+SCROLL_SIZE,"search_type=scan")) :
                "_search/scroll"+Configurator.asQueryString(Arrays.asList("scroll=1m","scroll_id="+scrollId)) );
        return Configurator.request(url).buildGet().invoke().readEntity(String.class);
    }
}
