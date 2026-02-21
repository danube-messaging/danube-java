package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Partitioned producer (3 partitions): sends 3 messages and verifies the
 * consumer receives all of them with partition coverage.
 */
class PartitionedBasicIT {

    private void runPartitionedBasic(String topicPrefix, SubType subType) {
        DanubeClient client = newClient();
        String topic = uniqueTopic(topicPrefix);
        int partitions = 3;

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_part_basic")
                .withPartitions(partitions)
                .build();
        producer.create();

        Consumer consumer = client.newConsumer()
                .withTopic(topic)
                .withConsumerName("cons_part_basic")
                .withSubscription("sub_part_basic")
                .withSubscriptionType(subType)
                .build();
        consumer.subscribe();

        try {
            var publisher = consumer.receive();

            Thread.sleep(300);

            String[] expected = {"Hello Danube 1", "Hello Danube 2", "Hello Danube 3"};
            for (String body : expected) {
                producer.send(body.getBytes(), Map.of());
            }

            List<StreamMessage> messages = receiveMessages(publisher, expected.length, Duration.ofSeconds(10));

            // Verify all payloads received
            Set<String> received = new HashSet<>();
            Set<String> partsSeen = new HashSet<>();
            for (StreamMessage msg : messages) {
                received.add(new String(msg.payload()));
                partsSeen.add(msg.messageId().topicName());
                consumer.ack(msg);
            }

            for (String body : expected) {
                assertTrue(received.contains(body), "missing message: " + body);
            }

            // Verify partition coverage
            for (int i = 0; i < partitions; i++) {
                String partName = topic + "-part-" + i;
                assertTrue(partsSeen.contains(partName), "missing partition: " + partName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } finally {
            consumer.close();
            client.close();
        }
    }

    @Test
    void partitionedBasicExclusive() {
        runPartitionedBasic("/default/part_basic_excl", SubType.EXCLUSIVE);
    }

    @Test
    void partitionedBasicShared() {
        runPartitionedBasic("/default/part_basic_shared", SubType.SHARED);
    }
}
