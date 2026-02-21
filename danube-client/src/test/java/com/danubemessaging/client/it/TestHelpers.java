package com.danubemessaging.client.it;

import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.model.StreamMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared helpers for integration tests.
 */
final class TestHelpers {

    static final String DEFAULT_BROKER_URL = "http://127.0.0.1:6650";

    private TestHelpers() {
    }

    static String brokerUrl() {
        String url = System.getenv("DANUBE_BROKER_URL");
        return (url != null && !url.isBlank()) ? url : DEFAULT_BROKER_URL;
    }

    static DanubeClient newClient() {
        return DanubeClient.builder()
                .serviceUrl(brokerUrl())
                .build();
    }

    static String uniqueTopic(String prefix) {
        return prefix + "-" + System.currentTimeMillis();
    }

    /**
     * Subscribes to a {@link Flow.Publisher} and collects exactly {@code count}
     * messages within {@code timeout}. Fails the test on timeout.
     */
    static List<StreamMessage> receiveMessages(Flow.Publisher<StreamMessage> publisher, int count, Duration timeout) {
        var collector = new MessageCollector(count);
        publisher.subscribe(collector);
        try {
            boolean ok = collector.latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!ok) {
                fail("Timeout: received only " + collector.messages.size() + "/" + count + " messages");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while waiting for messages");
        }
        return Collections.unmodifiableList(new ArrayList<>(collector.messages));
    }

    static StreamMessage receiveOne(Flow.Publisher<StreamMessage> publisher, Duration timeout) {
        return receiveMessages(publisher, 1, timeout).getFirst();
    }

    /**
     * A simple Flow.Subscriber that collects messages into a list and counts
     * down a latch when the expected count is reached.
     */
    static class MessageCollector implements Flow.Subscriber<StreamMessage> {
        final List<StreamMessage> messages = new CopyOnWriteArrayList<>();
        final CountDownLatch latch;
        private final int expected;
        private Flow.Subscription subscription;

        MessageCollector(int expected) {
            this.expected = expected;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(StreamMessage item) {
            messages.add(item);
            if (messages.size() >= expected) {
                latch.countDown();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            latch.countDown();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }
    }
}
