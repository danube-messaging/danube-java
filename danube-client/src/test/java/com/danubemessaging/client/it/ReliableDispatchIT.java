package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.DispatchStrategy;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Reliable dispatch: sends 20 identical 1 KB payloads and verifies exact
 * payload integrity. Acks are done inline during receive (broker waits
 * for ack before sending next message in reliable mode).
 */
class ReliableDispatchIT {

    private void runReliableBasic(String topicPrefix, SubType subType) throws Exception {
        DanubeClient client = newClient();
        String topic = uniqueTopic(topicPrefix);

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_reliable_basic")
                .withDispatchStrategy(DispatchStrategy.RELIABLE)
                .build();
        producer.create();

        Consumer consumer = client.newConsumer()
                .withTopic(topic)
                .withConsumerName("cons_rel_basic")
                .withSubscription("rel_sub_basic")
                .withSubscriptionType(subType)
                .build();
        consumer.subscribe();

        try {
            // Generate a ~1 KB payload
            byte[] blobData = new byte[1024];
            Arrays.fill(blobData, (byte) 'D');
            int messageCount = 20;

            // Attach subscriber BEFORE sending so the receive stream is ready
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);

            consumer.receive().subscribe(new TestHelpers.MessageCollector(messageCount) {
                @Override
                public void onNext(StreamMessage item) {
                    try {
                        assertArrayEquals(blobData, item.payload(),
                                "message " + count.get() + " payload mismatch");
                        consumer.ack(item);
                        if (count.incrementAndGet() >= messageCount) {
                            done.countDown();
                        }
                    } catch (Throwable t) {
                        error.set(t);
                        done.countDown();
                    }
                }
            });

            Thread.sleep(400);

            for (int i = 0; i < messageCount; i++) {
                producer.send(blobData, Map.of());
            }

            assertTrue(done.await(15, TimeUnit.SECONDS),
                    "Timeout: received " + count.get() + "/" + messageCount);

            if (error.get() != null) {
                fail("Assertion failed in subscriber: " + error.get().getMessage());
            }
        } finally {
            consumer.close();
            client.close();
        }
    }

    @Test
    void reliableDispatchExclusive() throws Exception {
        runReliableBasic("/default/reliable_basic_exclusive", SubType.EXCLUSIVE);
    }

    @Test
    void reliableDispatchShared() throws Exception {
        runReliableBasic("/default/reliable_basic_shared", SubType.SHARED);
    }
}
