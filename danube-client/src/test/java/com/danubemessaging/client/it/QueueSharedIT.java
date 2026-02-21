package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Queue semantics with Shared subscriptions: 3 consumers share the same
 * subscription name; 36 messages are distributed across them approximately
 * round-robin (each receives ~12).
 */
class QueueSharedIT {

    @Test
    void queueSharedRoundRobin() throws Exception {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/queue_shared");
        int total = 36;

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_queue_shared")
                .build();
        producer.create();

        String subName = "queue-shared-sub";
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        AtomicInteger received = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        List<Consumer> consumers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 3; i++) {
            String cname = "queue-cons-" + i;
            counts.put(cname, new AtomicInteger());
            Consumer cons = client.newConsumer()
                    .withTopic(topic)
                    .withConsumerName(cname)
                    .withSubscription(subName)
                    .withSubscriptionType(SubType.SHARED)
                    .build();
            cons.subscribe();
            consumers.add(cons);

            final String consumerName = cname;
            final Consumer consRef = cons;
            cons.receive().subscribe(new TestHelpers.MessageCollector(total) {
                @Override
                public void onNext(com.danubemessaging.client.model.StreamMessage item) {
                    consRef.ack(item);
                    counts.get(consumerName).incrementAndGet();
                    if (received.incrementAndGet() >= total) {
                        done.countDown();
                    }
                }
            });
        }

        Thread.sleep(400);

        // Send messages
        for (int i = 0; i < total; i++) {
            producer.send(("m" + i).getBytes(), Map.of());
        }

        assertTrue(done.await(15, TimeUnit.SECONDS),
                "Timeout: received " + received.get() + "/" + total);

        // Assert near round-robin: each consumer should get total/3
        int expected = total / 3;
        for (int i = 0; i < 3; i++) {
            String cname = "queue-cons-" + i;
            int got = counts.get(cname).get();
            assertEquals(expected, got, cname + " message count");
        }

        consumers.forEach(Consumer::close);
        client.close();
    }
}
