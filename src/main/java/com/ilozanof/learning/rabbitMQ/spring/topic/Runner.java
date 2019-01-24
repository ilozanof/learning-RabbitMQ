package com.ilozanof.learning.rabbitMQ.spring.topic;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class Runner implements CommandLineRunner {
    private final RabbitTemplate rabbitTemplate;
    private final Receiver receiver;

    public Runner(Receiver receiver, RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.receiver = receiver;
    }

    @Override
    public void run(String... args) throws Exception {
        final int NUM_MESSAGES = 50;
        for (int i = 0; i < NUM_MESSAGES; i++) {
            System.out.println("Sending message...");
            rabbitTemplate.convertAndSend(TopicApplication.topicExchangeName, "foo.bar.baz", "Hello from RAbbitMQ!");
            receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
        }
    }
}
