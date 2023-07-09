package test.confluent.services.Consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CountRecordsTopic {
    private final static String bootstrapServers = "localhost:9092";
    private final static String schemaRegistryURL = "http://localhost:8081";
    private final static List<String> topicsName = Arrays.asList("aemet-weather");
    private static KafkaConsumer<String, GenericRecord> consumer;

    public static void main(String[] args) {
        consumer = createConsumer();
        countRecords();
    }

    private static KafkaConsumer<String, GenericRecord> createConsumer(){
        Properties properties = new Properties();
        // Mandatory
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "AemetWeatherConsumer4");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        return new KafkaConsumer<>(properties);
    }

    public static void countRecords(){
        consumer.subscribe(topicsName);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            Map<String, Integer> numRecordsPerTopic = new HashMap<>();
            for(String topicName : topicsName) {
                numRecordsPerTopic.put(topicName, 0);
                int retries = 2;
                int currentRetries = 0;
                while (true) {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

                    if (records.count() == 0) {
                        currentRetries += 1;
                        if (currentRetries > retries) break;
                        Thread.sleep(2_000);
                        continue;
                    }

                    numRecordsPerTopic.put(topicName, numRecordsPerTopic.get(topicName) + records.count());
                }
            }

            for(Map.Entry<String, Integer> entry : numRecordsPerTopic.entrySet()){
                System.out.println("Num records in topic "+entry.getKey()+" is "+entry.getValue().toString());
            }

        } catch (WakeupException e) {
            System.out.println("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            System.out.println("Consumer has been closed correctly.");
        }
    }
}
