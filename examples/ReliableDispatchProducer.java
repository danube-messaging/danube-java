import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.DispatchStrategy;
import com.danubemessaging.client.Producer;

import java.util.Map;

/**
 * Reliable dispatch producer example: the broker waits for a consumer ack
 * before delivering the next message, guaranteeing ordered, lossless delivery.
 *
 * Run ReliableDispatchConsumer.java in a separate terminal to receive these messages.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class ReliableDispatchProducer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/reliable_topic";

    private static final String[] SUBJECTS = {
        "The Danube system", "Danube platform", "Danube messaging service", "The Danube application"
    };
    private static final String[] VERBS = {"processes", "handles", "manages", "delivers"};
    private static final String[] OBJECTS = {
        "messages efficiently", "data reliably", "requests quickly", "events seamlessly"
    };
    private static final String[] CONCLUSIONS = {
        "with high performance.", "at scale.", "in real-time.", "without issues."
    };

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        Producer producer = client.newProducer()
                .withTopic(TOPIC)
                .withName("prod_reliable")
                .withDispatchStrategy(DispatchStrategy.RELIABLE)
                .build();

        producer.create();
        System.out.println("Reliable dispatch producer created");

        for (int i = 0; i < 20; i++) {
            String message = String.format(
                    "%s %s %s, with low latency and high throughput. It is designed for reliability, %s",
                    SUBJECTS[i % SUBJECTS.length],
                    VERBS[i % VERBS.length],
                    OBJECTS[i % OBJECTS.length],
                    CONCLUSIONS[i % CONCLUSIONS.length]);

            long msgId = producer.send(message.getBytes(), Map.of());
            System.out.printf("Sent message #%d (id=%d)%n", i, msgId);
            Thread.sleep(1000);
        }

        System.out.println("Done");
        client.close();
    }
}
