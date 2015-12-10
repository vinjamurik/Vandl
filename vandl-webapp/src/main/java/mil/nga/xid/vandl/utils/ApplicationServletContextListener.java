package mil.nga.xid.vandl.utils;

import mil.nga.xid.vandl.dataflow.DataflowService;
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
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("vandl.properties");
            Configurator.setProperties(is);
            is.close();

            String kpath = Configurator.getProperties().getProperty("vandlKeyStoreFile");
            String kpass = Configurator.getProperties().getProperty("vandlKeyStorePassword");

            SSLContext sslContext = SslConfigurator.newInstance().keyStoreFile(kpath).keyPassword(kpass).trustStoreFile(kpath).trustStorePassword(kpass).createSSLContext();
            Configurator.setClient( ClientBuilder.newBuilder().sslContext(sslContext).build().register(HttpAuthenticationFeature.basic("es_admin","secret")) );

            mil.nga.xid.vandl.dataflow.utils.Configurator.init();
            DataflowService.init();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        Configurator.getClient().close();
    }
}
