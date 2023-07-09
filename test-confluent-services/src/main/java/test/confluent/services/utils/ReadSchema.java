package test.confluent.services.utils;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import test.confluent.services.Producer.ProducerAemet;
import test.confluent.services.Topic.CreateTopic;

import java.io.FileInputStream;

public class ReadSchema {
    private static final Logger log = LoggerFactory.getLogger(ReadSchema.class);

    public static Schema readSchema(String topic) {
        try {
            String schemaPath = ReadSchema.class.getClassLoader().getResource(topic + "_value.avsc").getFile();
            String schemaString = "";
            FileInputStream inputStream = new FileInputStream(schemaPath);
            try {
                schemaString = IOUtils.toString(inputStream);
            } finally {
                inputStream.close();
            }
            return new Schema.Parser().parse(schemaString);

        } catch (Exception err){
            throw new RuntimeException("Caught Exception: " + err.getMessage());
        }
    }
}
