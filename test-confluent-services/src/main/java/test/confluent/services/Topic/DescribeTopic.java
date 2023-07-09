package test.confluent.services.Topic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import test.confluent.services.AdminClientClass;

import java.util.*;
import java.util.stream.Collectors;

public class DescribeTopic
{
    private final static List<String> topicsDescribe = Arrays.asList("topic-test-1", "topic-test-2");

    public static void main( String[] args )
    {
        for(String topic : topicsDescribe) {
            describeTopic(topic);
            describeConfigsTopic(topic);
            System.out.println("------------------------------------------");
        }
    }

    static void describeTopic(final String topic) {

        final AdminClient adminClient = new AdminClientClass().getAdminClient();

        try {
            DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topic));
            for (Map.Entry<String, TopicDescription> entry : result.all().get().entrySet()) {
                System.out.println(entry.getValue().toString());
            }
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }

    static void describeConfigsTopic(final String topic) {

        final AdminClient adminClient = new AdminClientClass().getAdminClient();
        System.out.println("Getting configuration of topic "+topic);
        try {
            DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(
                    new ConfigResource(ConfigResource.Type.TOPIC, topic)));
            for (Map.Entry<ConfigResource, Config> entry : result.all().get().entrySet()) {
                Collection<ConfigEntry> configs = entry.getValue().entries();
                for(ConfigEntry config : configs){
                    System.out.println(config.name()+"="+config.value());
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }

    static boolean checkIfTopicExists(String topic) {
        final AdminClient adminClient = new AdminClientClass().getAdminClient();
        try {
            return adminClient.listTopics().names().get().stream().collect(Collectors.toList()).contains(topic);
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }

    }
}
