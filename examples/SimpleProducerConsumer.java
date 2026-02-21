import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

/**
 * Simple example showing basic producer/consumer without schema validation.
 * Demonstrates raw byte message passing with one producer and one consumer.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 *
 * Run: javac -cp danube-client-0.2.0.jar SimpleProducerConsumer.java
 *      java  -cp .:danube-client-0.2.0.jar SimpleProducerConsumer
 */
public class SimpleProducerConsumer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/simple_topic";
    private static final int MESSAGE_COUNT = 5;

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        // --- Producer ---
        Producer producer = client.newProducer()
                .withTopic(TOPIC)
                .withName("simple_producer")
                .build();

        producer.create();
        System.out.println("Producer created");

        // --- Consumer ---
        Consumer consumer = client.newConsumer()
                .withTopic(TOPIC)
                .withConsumerName("simple_consumer")
                .withSubscription("simple_subscription")
                .withSubscriptionType(SubType.EXCLUSIVE)
                .build();

        consumer.subscribe();
        System.out.println("Consumer subscribed");

        // Attach subscriber BEFORE producing so no messages are missed
        CountDownLatch done = new CountDownLatch(MESSAGE_COUNT);

        consumer.receive().subscribe(new Flow.Subscriber<>() {
            Flow.Subscription sub;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.sub = subscription;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamMessage msg) {
                String payload = new String(msg.payload());
                System.out.printf("Received: '%s' (topic offset: %d)%n",
                        payload, msg.messageId().topicOffset());
                consumer.ack(msg);
                done.countDown();
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Receive error: " + t.getMessage());
                while (done.getCount() > 0) done.countDown();
            }

            @Override
            public void onComplete() {}
        });

        // Produce on a virtual thread so the main thread can wait for messages
        Thread.ofVirtual().start(() -> {
            try {
                for (int i = 1; i <= MESSAGE_COUNT; i++) {
                    String message = "Hello Danube! Message #" + i;
                    long msgId = producer.send(message.getBytes(), Map.of());
                    System.out.printf("Sent: '%s' (id: %d)%n", message, msgId);
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                System.err.println("Send error: " + e.getMessage());
            }
        });

        done.await();
        System.out.printf("%nDemo completed — sent and received %d messages%n", MESSAGE_COUNT);

        consumer.close();
        client.close();
    }
}
