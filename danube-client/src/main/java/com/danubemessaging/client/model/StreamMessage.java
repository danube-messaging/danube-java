package com.danubemessaging.client.model;

import danube.DanubeApi;
import java.util.Map;

/**
 * Message delivered by a consumer receive stream.
 */
public record StreamMessage(
        long requestId,
        MessageId messageId,
        byte[] payload,
        long publishTime,
        String producerName,
        String subscriptionName,
        Map<String, String> attributes) {

    public StreamMessage {
        payload = payload == null ? new byte[0] : payload.clone();
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public static StreamMessage fromProto(DanubeApi.StreamMessage proto) {
        return new StreamMessage(
                proto.getRequestId(),
                MessageId.fromProto(proto.getMsgId()),
                proto.getPayload().toByteArray(),
                proto.getPublishTime(),
                proto.getProducerName(),
                proto.getSubscriptionName(),
                proto.getAttributesMap());
    }
}
