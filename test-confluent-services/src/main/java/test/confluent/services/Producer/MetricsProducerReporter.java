package test.confluent.services.Producer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.*;

public class MetricsProducerReporter implements Runnable {

    private final KafkaProducer<String, GenericRecord> producer;

    //Used to Filter just the metrics we want
    private final List<String> metricsNameFilter = Arrays.asList(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate", "record-queue-time-avg",
            "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight"
    );

    public MetricsProducerReporter(final KafkaProducer<String, GenericRecord> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            final Map<MetricName, ? extends Metric> metrics  = producer.metrics();
            displayMetrics(metrics);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                System.out.println("metrics interrupted");
                Thread.interrupted();
                break;
            }
        }
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        Map<String, String> metricsMap= new HashMap<>();
        metrics.entrySet().stream()
                .filter(metricNameEntry -> metricsNameFilter.contains(metricNameEntry.getKey().name()))
                //Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry ->
                        !Double.isInfinite((Double) metricNameEntry.getValue().metricValue()) &&
                                !Double.isNaN((Double) metricNameEntry.getValue().metricValue()) &&
                                (Double) metricNameEntry.getValue().metricValue() != 0
                )
                .forEach(entry -> {
                    String name = entry.getKey().name();
                    String value = entry.getValue().metricValue().toString();
                    metricsMap.put(name, value);
                });
        System.out.println(metricsMap);
        System.out.println("\n");
    }
}
