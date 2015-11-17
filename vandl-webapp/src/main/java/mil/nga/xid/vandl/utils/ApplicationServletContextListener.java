package mil.nga.xid.vandl.utils;

import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.net.ssl.SSLContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.client.ClientBuilder;
import java.io.IOException;
import java.io.InputStream;

@WebListener
public final class ApplicationServletContextListener implements javax.servlet.ServletContextListener{

    public void contextInitialized(ServletContextEvent servletContextEvent){
        Configurator.setServletContext(servletContextEvent.getServletContext());
        /*String keyPath = Configurator.getServletContext().getRealPath("/WEB-INF/classes/truststore.jks");
        SSLContext sslContext = SslConfigurator.newInstance().trustStoreFile(keyPath).trustStorePassword("secret").trustStoreType("JKS").createSSLContext();
        Configurator.setClient(ClientBuilder.newBuilder().sslContext(sslContext).build());*/
        Configurator.setClient(ClientBuilder.newClient().register(HttpAuthenticationFeature.basic("es_admin","secret")));
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("vandl.properties");
            Configurator.setProperties(is);
            is.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        Configurator.getClient().close();
    }
}
