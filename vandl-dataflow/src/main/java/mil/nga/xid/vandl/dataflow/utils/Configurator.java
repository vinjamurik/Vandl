package mil.nga.xid.vandl.dataflow.utils;

import com.esotericsoftware.yamlbeans.YamlReader;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public final class Configurator {
    private static Map<String,Object> config;

    private Configurator(){}

    public static void init() throws IOException{
        YamlReader yr = new YamlReader(new InputStreamReader(Configurator.class.getClassLoader().getResourceAsStream("vandl-dataflow.yml")));
        config = (Map)yr.read();
        yr.close();
    }

    public static Object getProperty(String property){
        return config.get(property);
    }

    public static Properties readPropertiesFromStream(InputStream is) throws IOException{
        Properties prop = new Properties();
        prop.load(is);
        is.close();
        return prop;
    }
}
