package com.danubemessaging.client.internal.connection;

import java.net.URI;

/**
 * Broker identity and effective connection target.
 */
public record BrokerAddress(URI connectUrl, URI brokerUrl, boolean proxy) {}
