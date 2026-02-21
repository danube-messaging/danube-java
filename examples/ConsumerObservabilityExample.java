import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.ConsumerEventListener;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import java.time.Duration;

/**
 * Example: subscribe with observability hooks and print received schema metadata.
 */
public final class ConsumerObservabilityExample {
    private ConsumerObservabilityExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl("http://localhost:6650")
                .build();

        try {
            Consumer consumer = client.newConsumer()
                    .withTopic("/default/user-events")
                    .withName("example-consumer")
                    .withSubscription("example-sub")
                    .withSubscriptionType(SubType.EXCLUSIVE)
                    .withEventListener(new LoggingListener())
                    .build();

            consumer.receive().subscribe(new java.util.concurrent.Flow.Subscriber<>() {
                @Override
                public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(StreamMessage item) {
                    System.out.printf(
                            "Received topic=%s schemaId=%s schemaVersion=%s%n",
                            item.messageId().topicName(),
                            item.schemaId(),
                            item.schemaVersion());
                    consumer.ack(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println("Receive stream terminated: " + throwable.getMessage());
                }

                @Override
                public void onComplete() {
                    System.out.println("Receive stream completed");
                }
            });

            consumer.subscribe();
            Thread.sleep(Duration.ofSeconds(30));
            consumer.close();
        } finally {
            client.close();
        }
    }

    private static final class LoggingListener implements ConsumerEventListener {
        @Override
        public void onConsumerSubscribed(String topic, String consumerName, long consumerId) {
            System.out.printf("Subscribed topic=%s consumer=%s id=%d%n", topic, consumerName, consumerId);
        }

        @Override
        public void onMessageReceived(String topic, String consumerName, StreamMessage message) {
            System.out.printf("Message received topic=%s requestId=%d%n", topic, message.requestId());
        }

        @Override
        public void onMessageAcked(String topic, String consumerName, StreamMessage message) {
            System.out.printf("Acked topic=%s message=%s%n", topic, message.messageId());
        }

        @Override
        public void onConsumerError(String topic, String consumerName, Throwable error, boolean retryable) {
            System.err.printf(
                    "Consumer error topic=%s retryable=%s cause=%s%n",
                    topic,
                    retryable,
                    error.getMessage());
        }

        @Override
        public void onFatalReceiveError(String topic, String consumerName, Throwable error) {
            System.err.printf("Fatal receive error topic=%s cause=%s%n", topic, error.getMessage());
        }

        @Override
        public void onConsumerClosed(String topic, String consumerName) {
            System.out.printf("Consumer closed topic=%s consumer=%s%n", topic, consumerName);
        }
    }
}
