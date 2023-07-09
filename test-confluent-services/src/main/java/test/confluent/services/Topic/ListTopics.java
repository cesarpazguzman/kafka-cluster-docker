package test.confluent.services.Topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import test.confluent.services.AdminClientClass;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ListTopics
{
    private final static boolean listInternals = false;
    public static void main( String[] args )
    {
        listTopics();
    }

    static void listTopics() {
        final AdminClient adminClient = new AdminClientClass().getAdminClient();
        try {
            List<String> topicNames = adminClient.listTopics(new ListTopicsOptions().listInternal(listInternals))
                    .names().get().stream().collect(Collectors.toList());
            topicNames.forEach(System.out::println);
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }

    }
}
