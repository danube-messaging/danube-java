package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Fan-out with Exclusive subscriptions: 3 consumers each with a unique
 * subscription receive ALL messages (broadcast). 24 messages sent;
 * each consumer must receive all 24.
 */
class FanoutExclusiveIT {

    @Test
    void fanoutExclusive() throws Exception {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/fanout_exclusive");
        int total = 24;

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_fanout_exclusive")
                .build();
        producer.create();

        record ConsumerEntry(String name, Consumer consumer, TestHelpers.MessageCollector collector) {
        }

        List<ConsumerEntry> consumers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 3; i++) {
            String cname = "fanout-cons-" + i;
            String subName = "fanout-sub-" + i;
            Consumer cons = client.newConsumer()
                    .withTopic(topic)
                    .withConsumerName(cname)
                    .withSubscription(subName)
                    .withSubscriptionType(SubType.EXCLUSIVE)
                    .build();
            cons.subscribe();
            var collector = new TestHelpers.MessageCollector(total);
            cons.receive().subscribe(collector);
            consumers.add(new ConsumerEntry(cname, cons, collector));
        }

        Thread.sleep(400);

        // Send messages
        for (int i = 0; i < total; i++) {
            producer.send(("m" + i).getBytes(), Map.of());
        }

        // Wait for each consumer to receive all messages
        for (var ce : consumers) {
            boolean ok = ce.collector().latch.await(20, TimeUnit.SECONDS);
            if (!ok) {
                fail(ce.name() + " received only " + ce.collector().messages.size() + "/" + total);
            }
        }

        // Assert every consumer got every message
        for (var ce : consumers) {
            Set<String> payloads = ConcurrentHashMap.newKeySet();
            for (StreamMessage msg : ce.collector().messages) {
                payloads.add(new String(msg.payload()));
                ce.consumer().ack(msg);
            }
            assertEquals(total, payloads.size(), ce.name() + " message count");
            for (int i = 0; i < total; i++) {
                assertTrue(payloads.contains("m" + i), ce.name() + " missing m" + i);
            }
        }

        consumers.forEach(ce -> ce.consumer().close());
        client.close();
    }
}
