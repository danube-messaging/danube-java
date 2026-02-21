package com.danubemessaging.client.internal.retry;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry behavior and retry-related helpers.
 */
public final class RetryManager {
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long DEFAULT_BASE_BACKOFF_MS = 200;
    private static final long DEFAULT_MAX_BACKOFF_MS = 5_000;

    private static final Metadata.Key<String> BROKER_HEADER = Metadata.Key.of("x-danube-broker-url",
            Metadata.ASCII_STRING_MARSHALLER);

    private final int maxRetries;
    private final long baseBackoffMs;
    private final long maxBackoffMs;

    public RetryManager(int maxRetries, long baseBackoffMs, long maxBackoffMs) {
        this.maxRetries = maxRetries <= 0 ? DEFAULT_MAX_RETRIES : maxRetries;
        this.baseBackoffMs = baseBackoffMs <= 0 ? DEFAULT_BASE_BACKOFF_MS : baseBackoffMs;
        this.maxBackoffMs = maxBackoffMs <= 0 ? DEFAULT_MAX_BACKOFF_MS : maxBackoffMs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public boolean isRetryable(Throwable error) {
        Status.Code code = extractStatusCode(error);
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.RESOURCE_EXHAUSTED;
    }

    public boolean isUnrecoverable(Throwable error) {
        Status.Code code = extractStatusCode(error);
        return code == Status.Code.ABORTED
                || code == Status.Code.FAILED_PRECONDITION
                || code == Status.Code.NOT_FOUND
                || code == Status.Code.PERMISSION_DENIED
                || code == Status.Code.UNAUTHENTICATED;
    }

    public Duration calculateBackoff(int attempt) {
        long linear = baseBackoffMs * (attempt + 1L);
        long capped = Math.min(linear, maxBackoffMs);
        long min = Math.max(1L, capped / 2L);
        long jittered = ThreadLocalRandom.current().nextLong(min, capped + 1L);
        return Duration.ofMillis(jittered);
    }

    public static void insertProxyHeader(Metadata metadata, String brokerUrl, boolean proxy) {
        if (proxy) {
            metadata.put(BROKER_HEADER, brokerUrl);
        }
    }

    private static Status.Code extractStatusCode(Throwable error) {
        Throwable current = error;
        while (current != null) {
            Status.Code code = Status.fromThrowable(current).getCode();
            if (current instanceof StatusRuntimeException || current instanceof StatusException
                    || code != Status.Code.UNKNOWN) {
                return code;
            }
            current = current.getCause();
        }
        return Status.Code.UNKNOWN;
    }
}
