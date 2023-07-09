package test.confluent.services.Topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import test.confluent.services.AdminClientClass;

import java.util.Arrays;
import java.util.List;

public class DeleteTopic
{
    private final static List<String> topicsDelete = Arrays.asList("topic-test", "topic-test-1", "topic-test-2");

    public static void main( String[] args )
    {
        deleteTopic("topic-test");
    }

    static void deleteTopic(final String topic) {
        System.out.println("Deleting topic "+topic);

        final AdminClient adminClient = new AdminClientClass().getAdminClient();
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(topicsDelete);
            result.all().get();

            System.out.println("Topics are successfully removed.");
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }
}
