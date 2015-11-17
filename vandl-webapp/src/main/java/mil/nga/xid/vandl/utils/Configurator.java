package mil.nga.xid.vandl.utils;

import javax.servlet.ServletContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public final class Configurator {
    private static ServletContext servletContext;
    private static Properties properties;
    private static Client client;

    public static ServletContext getServletContext() {
        return servletContext;
    }

    public static void setServletContext(ServletContext servletContext) {
        Configurator.servletContext = servletContext;
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void setProperties(InputStream is) throws IOException{
        Configurator.properties = new Properties();
        properties.load(is);
    }

    protected static void setClient(Client client) {
        Configurator.client = client;
    }

    static Client getClient(){
        return client;
    }

    public static Invocation.Builder request(String s){
        return client.target(s).request();
    }

    public static String asQueryString(List<String> qs){
        StringBuilder b = new StringBuilder("?");
        for(String q : qs){
            b.append(q).append("&");
        }
        return b.toString();
    }
}
