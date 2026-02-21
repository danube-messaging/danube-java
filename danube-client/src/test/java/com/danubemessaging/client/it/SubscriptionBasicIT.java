package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic subscription integration tests: one producer, one consumer,
 * send/receive/ack on a non-partitioned topic.
 */
class SubscriptionBasicIT {

    private void runBasicSubscription(String topicPrefix, SubType subType) {
        DanubeClient client = newClient();
        String topic = uniqueTopic(topicPrefix);

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_basic")
                .build();
        producer.create();

        Consumer consumer = client.newConsumer()
                .withTopic(topic)
                .withConsumerName("consumer_basic")
                .withSubscription("test_sub_basic")
                .withSubscriptionType(subType)
                .build();
        consumer.subscribe();

        try {
            var publisher = consumer.receive();

            Thread.sleep(300);

            byte[] payload = "Hello Danube".getBytes();
            producer.send(payload, Map.of());

            StreamMessage msg = receiveOne(publisher, Duration.ofSeconds(10));

            assertEquals("Hello Danube", new String(msg.payload()));

            consumer.ack(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } finally {
            consumer.close();
            client.close();
        }
    }

    @Test
    void basicSubscriptionShared() {
        runBasicSubscription("/default/sub_basic_shared", SubType.SHARED);
    }

    @Test
    void basicSubscriptionExclusive() {
        runBasicSubscription("/default/sub_basic_exclusive", SubType.EXCLUSIVE);
    }
}
