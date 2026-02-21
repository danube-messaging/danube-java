package com.danubemessaging.client.model;

import danube.DanubeApi;

/**
 * Message identifier metadata carried with every stream message.
 */
public record MessageId(long producerId, String topicName, String brokerAddress, long topicOffset) {

    public static MessageId fromProto(DanubeApi.MsgID msgId) {
        return new MessageId(
                msgId.getProducerId(),
                msgId.getTopicName(),
                msgId.getBrokerAddr(),
                msgId.getTopicOffset());
    }

    public DanubeApi.MsgID toProto() {
        return DanubeApi.MsgID.newBuilder()
                .setProducerId(producerId)
                .setTopicName(topicName)
                .setBrokerAddr(brokerAddress)
                .setTopicOffset(topicOffset)
                .build();
    }
}
