package test.confluent.services.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class ReadYaml {

    public static Map<String, List<Map<String, Object>>> readFile(String filename){
        try {
            InputStream inputStream = new FileInputStream(filename);
            Yaml yaml = new Yaml();
            Map<String, List<Map<String, Object>>> data = yaml.load(inputStream);
            System.out.println(data);
            return data;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }
}
