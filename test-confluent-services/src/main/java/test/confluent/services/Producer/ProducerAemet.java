package test.confluent.services.Producer;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import test.confluent.services.utils.ReadAemetAPI;
import test.confluent.services.utils.ReadSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ProducerAemet {
    private static final Logger log = LoggerFactory.getLogger(ProducerAemet.class);
    private final static String bootstrapServers = "localhost:9092";
    private final static String schemaRegistryURL = "http://localhost:8081";
    private static KafkaProducer<String, GenericRecord> producer = null;
    private final static String topicName = "aemet-weather";
    private final static ExecutorService executorService = Executors.newFixedThreadPool(1);

    public static void main(String[] args) {
        System.out.println("Creating the producer instance...");
        producer = getProducerInstance();
        executorService.submit(new MetricsProducerReporter(producer));

        System.out.println("Generating a new record using the defined schema...");
        GenericRecord record = ReadAemetAPI.newRecord(ReadSchema.readSchema(topicName));

        System.out.println("Sending the record to topic aemet-weather...");
        sendRecord(record);

        System.out.println("Closing the producer...");

        producer.flush();
        producer.close();

    }

    private static KafkaProducer<String, GenericRecord> getProducerInstance(){
        Properties properties = new Properties();
        // Mandatory
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Optional
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //Message's schema is registered in the schema registry
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        return new KafkaProducer<>(properties);
    }

    private static void sendRecord(GenericRecord record){
        try {
            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(
                    topicName, (String) record.get("locality"), record);

            Future<RecordMetadata> res = producer.send(producerRecord, (recordMetadata, e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    Map<String, Object> recordMetadataMap = new HashMap<>();
                    recordMetadataMap.put("Topic", recordMetadata.topic());
                    recordMetadataMap.put("Key", producerRecord.key());
                    recordMetadataMap.put("Partition", recordMetadata.partition());
                    recordMetadataMap.put("Offset", recordMetadata.offset());
                    recordMetadataMap.put("Timestamp", recordMetadata.timestamp());

                    // the record was successfully sent
                    System.out.println(recordMetadataMap);
                } else {
                    System.out.println("Error while producing: " + e);
                }
            });

            res.get();
        } catch(SerializationException e) {
            log.error("Error in the serialization. This record should be sent to a dead letter queue: "+e.getCause());
        } catch (TimeoutException e) {
            // handle timeout exception
            log.error("Timeout occurred while sending message to Kafka: "+e.getMessage());
        } catch (Exception e) {
            log.error("An exception has happened while sending message to Kafka"+e.getMessage());
        }
    }
}
