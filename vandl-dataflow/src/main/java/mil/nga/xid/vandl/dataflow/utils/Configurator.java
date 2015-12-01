package mil.nga.xid.vandl.dataflow.utils;

import com.esotericsoftware.yamlbeans.YamlReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Configurator {
    private static Map<String,Object> config;

    public static void init(){
        try{
            YamlReader yr = new YamlReader(new FileReader("vandl-dataflow/src/main/resources/vandl-dataflow.yml"));
            config = (Map)yr.read();
            yr.close();
        }catch(IOException e){
            Logger.getAnonymousLogger().log(Level.WARNING,e.getMessage());
        }
    }

    public void destroy(){

    }

    public static Object getProperty(String property){
        return config.get(property);
    }
}
