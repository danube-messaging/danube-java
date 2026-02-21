import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;

import java.util.Map;

/**
 * Partitioned producer example: creates a producer with 3 partitions and
 * round-robins messages across them.
 *
 * Run PartitionsConsumer.java in a separate terminal to receive these messages.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class PartitionsProducer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/partitioned_topic";

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        Producer producer = client.newProducer()
                .withTopic(TOPIC)
                .withName("prod_part")
                .withPartitions(3)
                .build();

        producer.create();
        System.out.println("Partitioned producer created (3 partitions)");

        for (int i = 0; i < 20; i++) {
            String payload = "Hello Danube " + i;
            long msgId = producer.send(payload.getBytes(), Map.of());
            System.out.printf("Sent message #%d (id=%d): %s%n", i, msgId, payload);
            Thread.sleep(500);
        }

        System.out.println("Done");
        client.close();
    }
}
