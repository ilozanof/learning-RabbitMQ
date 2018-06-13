package com.ilozanof.learning.rabbitMQ.topic;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SignalReceiver {

    private Logger logger = LoggerFactory.getLogger(SignalReceiver.class.getName());

    private final String EXCHANGE_NAME = "topicExchange";
    private final String QUEUE_NAMES[] = {"dev", "test", "prod"};
    private Connection connection;
    private Channel channel;

    public SignalReceiver() throws Exception {
        logger.debug("> Initializing Log Consumer...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        // We set up the Exchange..
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        // We set up the Queues, and the binding to the exchange...
        for (int i = 0; i < QUEUE_NAMES.length; i++) {
            channel.queueDeclare(QUEUE_NAMES[i], false, false, false, null);
            String routingKey = "#." + QUEUE_NAMES[i];
            channel.queueBind(QUEUE_NAMES[i], EXCHANGE_NAME, routingKey);
        }
    }

    public void setUpConsumers() throws Exception {
        // We set up a Consumer for each Queue.
        // Here we are interested in the routing features of the RabbitMQ TOPICS, not
        // in the consumption, so we use the same Consumer for all of them...

        for (int i = 0; i < QUEUE_NAMES.length; i++) {
            String consumerId = "ConsumerOf" + QUEUE_NAMES[i];
            Consumer consumer = new SignalConsumer(consumerId, channel);
            channel.basicConsume(QUEUE_NAMES[i], true, consumer);
        }
    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
    }
}
