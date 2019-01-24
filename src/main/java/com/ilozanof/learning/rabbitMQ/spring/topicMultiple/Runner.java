package com.ilozanof.learning.rabbitMQ.spring.topicMultiple;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Runner implements CommandLineRunner {
    private final RabbitTemplate rabbitTemplate;

    public Runner(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendMessage(String message, String topic) throws Exception {
        System.out.println("Sending the message to " + topic + "...");
        rabbitTemplate.convertAndSend(TopicMultipleApplication.topicExchangeName, topic, message);
    }

    @Override
    public void run(String ...args) throws Exception {
        String message = "This is a test message...";
        int NUM_LOOPS = 10;
        for (int i = 0; i < NUM_LOOPS; i++) {
            sendMessage(message,"user.important.error");
            sendMessage(message,"user.important");
            sendMessage(message,"user.error");
            sendMessage(message,"user");
            sendMessage(message,"");
        } // for...


    }
}
