package mil.nga.xid.vandl.webapp.utils;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

public class HttpsRequestFactory extends HttpComponentsClientHttpRequestFactory {

    public HttpsRequestFactory(String path, String pass) throws Exception{
        super();
        BasicCredentialsProvider bc = new BasicCredentialsProvider();
        bc.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("es_admin","secret"));
        setHttpClient(HttpClientBuilder.create()
                .setSSLContext(buildSSLContext(path,pass))
                .setDefaultCredentialsProvider(bc)
                .build());
    }

    private SSLContext buildSSLContext(String path, String pass) throws Exception{
        InputStream keyStoreStream = new FileInputStream(new File(path));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(keyStoreStream, pass.toCharArray());
        kmf.init(keyStore, pass.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(keyStore);
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        keyStoreStream.close();
        return sslContext;
    }
}
