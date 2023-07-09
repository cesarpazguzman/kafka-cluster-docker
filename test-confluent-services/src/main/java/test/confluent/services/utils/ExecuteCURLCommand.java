package test.confluent.services.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ExecuteCURLCommand {
    public static String execute(String command){
        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader =  new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ( (line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            return builder.toString();
        } catch (IOException e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }
}
