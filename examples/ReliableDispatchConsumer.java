import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reliable dispatch consumer example: the broker waits for each ack before
 * delivering the next message — guaranteeing ordered, lossless delivery.
 *
 * Start this consumer BEFORE running ReliableDispatchProducer.java.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class ReliableDispatchConsumer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/reliable_topic";

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        Consumer consumer = client.newConsumer()
                .withTopic(TOPIC)
                .withConsumerName("cons_reliable")
                .withSubscription("subs_reliable")
                .withSubscriptionType(SubType.EXCLUSIVE)
                .build();

        consumer.subscribe();
        System.out.println("Consumer subscribed — waiting for messages...");

        AtomicLong totalReceivedBytes = new AtomicLong();
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
                long total = totalReceivedBytes.addAndGet(msg.payload().length);
                System.out.printf("Received: %s | offset: %d | total bytes: %d%n",
                        new String(msg.payload()),
                        msg.messageId().topicOffset(),
                        total);
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
