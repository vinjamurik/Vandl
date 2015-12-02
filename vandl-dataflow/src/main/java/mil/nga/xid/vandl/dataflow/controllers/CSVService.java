package mil.nga.xid.vandl.dataflow.controllers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public final class CSVService {
    public static CSVParser readFromFile(String fileLocation,String[] headers) throws IOException{
        File f = new File(fileLocation);
        System.out.println(f.getAbsolutePath());
        if(!f.exists()){
            throw new FileNotFoundException("File does not exist");
        }
        if(!f.canRead()){
            throw new IOException("READ permission denied on file");
        }
        return headers == null ? CSVFormat.DEFAULT.withHeader().parse(new FileReader(f)) : CSVFormat.DEFAULT.withHeader(headers).parse(new FileReader(f));
    }

    public static CSVParser readFromFile(String fileLocation) throws IOException{
        return readFromFile(fileLocation,null);
    }
}
