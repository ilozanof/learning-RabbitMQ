package com.ilozanof.learning.rabbitMQ.spring.scalable;

/**
 * Interface for the Queue Consumer.
 */
public interface Consumer {
    void consume(String message);
}
