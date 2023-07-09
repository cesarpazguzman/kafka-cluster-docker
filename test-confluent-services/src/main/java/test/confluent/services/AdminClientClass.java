package test.confluent.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class AdminClientClass {
    private String bootstrapServers = "localhost:9092";
    private String securityProtocol = "PLAINTEXT";
    private AdminClient adminClient = null;

    /*
    private String securityProtocol = "SASL_SSL";
    private String saslMechanisms = "PLAIN";
    private String saslUsername = "test";
    private String saslPassword = "****";
    private String sslEndpointIdentificationAlgorithm = " ";
     */

    public AdminClientClass() {

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);

        this.adminClient = AdminClient.create(properties);
    }

    public AdminClient getAdminClient(){ return this.adminClient; }
}
