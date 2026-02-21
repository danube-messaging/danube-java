import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

/**
 * JSON consumer example: subscribes to a topic and prints received JSON payloads.
 *
 * Start this consumer BEFORE running JsonProducer.java.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class JsonConsumer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/json_topic";

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        Consumer consumer = client.newConsumer()
                .withTopic(TOPIC)
                .withConsumerName("cons_json")
                .withSubscription("subs_json")
                .withSubscriptionType(SubType.EXCLUSIVE)
                .build();

        consumer.subscribe();
        System.out.println("Consumer subscribed — waiting for messages...");

        // Infinite receive loop — press Ctrl+C to stop
        CountDownLatch shutdown = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            consumer.close();
            client.close();
            shutdown.countDown();
        }));

        consumer.receive().subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamMessage msg) {
                String json = new String(msg.payload());
                System.out.printf("Received (schema_version=%d): %s%n",
                        msg.schemaVersion() != null ? msg.schemaVersion() : 0, json);
                consumer.ack(msg);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Receive error: " + t.getMessage());
                shutdown.countDown();
            }

            @Override
            public void onComplete() {
                shutdown.countDown();
            }
        });

        shutdown.await();
    }
}
