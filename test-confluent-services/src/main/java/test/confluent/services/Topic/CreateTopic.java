package test.confluent.services.Topic;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import test.confluent.services.AdminClientClass;
import test.confluent.services.utils.ReadYaml;

public class CreateTopic
{
    public static void main( String[] args )
    {
        URL config_filename = CreateTopic.class.getClassLoader().getResource("create-topics-configuration.yaml");
        List<Map<String, Object>> topics_create_config = ReadYaml.readFile(config_filename.getFile()).get("topics-configuration");

        for(Map<String, Object> topic_create_config : topics_create_config){
            String topic_name = (String) topic_create_config.get("name");
            Integer num_partitions = (Integer) topic_create_config.get("num.partitions");
            Integer replication_factor = (Integer) topic_create_config.get("replication.factor");
            Map<String, String> additional_config = (Map<String, String>) topic_create_config.get("additional-config");
            createTopic(topic_name, num_partitions, replication_factor, additional_config);
        }
    }

    static void createTopic(final String topic, final int partitions,
                            final int replication, Map<String, String> additional_config) {

        if (!DescribeTopic.checkIfTopicExists(topic)) {
            System.out.println("Creating topic { name: " + topic + ", partitions: " + partitions + ",  replication: " + replication + "}");

            final AdminClient adminClient = new AdminClientClass().getAdminClient();

            final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
            newTopic.configs(additional_config);
            try {
                final CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
                result.all().get();
            } catch (final Exception e) {
                throw new RuntimeException("Caught Exception: " + e.getMessage());
            }
        } else {
            System.out.println("Topic "+topic+" already exists.");
        }
    }
}
