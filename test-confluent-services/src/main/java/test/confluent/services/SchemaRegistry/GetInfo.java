package test.confluent.services.SchemaRegistry;

import test.confluent.services.utils.ExecuteCURLCommand;

public class GetInfo {
    public static void main(String[] args) {
        String subjects = ExecuteCURLCommand.execute("curl -X GET http://localhost:8081/subjects");
        System.out.println("Subjects defined in the schema registry: "+subjects);

        String versions = ExecuteCURLCommand.execute(
                "curl -X GET http://localhost:8081/subjects/aemet-weather-value/versions");
        System.out.println("Versions for the aemet-weather-value subject: "+versions);

        String schemaVersion1 = ExecuteCURLCommand.execute(
                "curl -X GET http://localhost:8081/subjects/aemet-weather-value/versions/1");
        System.out.println("Aemet-weather subject version 1: "+schemaVersion1);

        String schemaAemet = ExecuteCURLCommand.execute("curl -X GET http://localhost:8081/schemas/ids/1");
        System.out.println("Schema id=1: "+schemaAemet);

        String compatibility = ExecuteCURLCommand.execute(
                "curl -X GET http://localhost:8081/config/aemet-weather-value");
        System.out.println("Compatibility: "+compatibility);
        if (compatibility.contains("does not have subject-level compatibility configured")){
            ExecuteCURLCommand.execute(
                    "curl -X PUT -H \"Content-Type: application/vnd.schemaregistry.v1+json\" --data " +
                            "\"{\\\"compatibility\\\": \\\"BACKWARD\\\"}\" http://localhost:8081/config/aemet-weather-value");

            compatibility = ExecuteCURLCommand.execute(
                    "curl -X GET http://localhost:8081/config/aemet-weather-value");
            System.out.println("Compatibility changed: "+compatibility);
        }

        String schemaTypes = ExecuteCURLCommand.execute("curl -X GET http://localhost:8081/schemas/types");
        System.out.println("Schema types: "+schemaTypes);
    }
}
