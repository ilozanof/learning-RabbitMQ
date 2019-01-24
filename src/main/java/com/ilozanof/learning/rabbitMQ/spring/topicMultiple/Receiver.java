package com.ilozanof.learning.rabbitMQ.spring.topicMultiple;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class Receiver {
    @RabbitListener(queues = {TopicMultipleApplication.topicQueueImportant})
    public void receiveMessageImportant(String message) {
        try {
            System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() + " > Processing message IMPORTANT...");
            Thread.sleep(500);
            System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() +  " > Processing message IMPORTANT...Done");
        } catch (Exception e) {e.printStackTrace();}

    }

    @RabbitListener(queues = {TopicMultipleApplication.topicQueueError})
    public void receiveMessageError (String message) {
        try {
            System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() +   " > Processing message ERROR...");
            Thread.sleep(500);
            System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() +   " > Processing message ERROR...Done");
        } catch (Exception e) {e.printStackTrace();}
    }
}
