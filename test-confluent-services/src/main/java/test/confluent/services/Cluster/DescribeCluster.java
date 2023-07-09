package test.confluent.services.Cluster;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import test.confluent.services.AdminClientClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class DescribeCluster {
    public static void main( String[] args )
    {
        System.out.println("Getting the cluster information");
        describeCluster();
        describeConfigsCluster();
    }

    static void describeCluster() {

        final AdminClient adminClient = new AdminClientClass().getAdminClient();

        try {
            DescribeClusterResult result = adminClient.describeCluster();
            System.out.println("ClusterId="+result.clusterId().get());
            System.out.println("Controller="+result.controller().get().toString());
            System.out.println("Nodes="+result.nodes().get());
            System.out.println("AclOperations="+result.authorizedOperations().get());
        } catch (final Exception e) {
            throw new RuntimeException("Caught Exception: " + e.getMessage());
        }
    }

    static void describeConfigsCluster() {

        final AdminClient adminClient = new AdminClientClass().getAdminClient();
        try {
            DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(
                    new ConfigResource(ConfigResource.Type.BROKER, "1")));
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
}
