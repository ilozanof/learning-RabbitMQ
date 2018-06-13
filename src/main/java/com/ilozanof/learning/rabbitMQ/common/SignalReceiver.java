package com.ilozanof.learning.rabbitMQ.common;

public interface SignalReceiver {
    void setUpConsumers() throws Exception;
    void close() throws Exception;
}
