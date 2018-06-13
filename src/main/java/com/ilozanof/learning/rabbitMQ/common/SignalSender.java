package com.ilozanof.learning.rabbitMQ.common;

public interface SignalSender {
    void send(String signal, String syste, String env) throws Exception;
    void close() throws Exception;
}
