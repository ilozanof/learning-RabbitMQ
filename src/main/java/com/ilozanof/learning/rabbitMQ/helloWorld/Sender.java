package com.ilozanof.learning.rabbitMQ.helloWorld;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Sender {

    private final Logger logger = LoggerFactory.getLogger(Sender.class.getName());

    private final static String QUEUE_NAME = "hello";

    private Connection connection;
    private Channel channel;

    public Sender() throws Exception {
        logger.debug("> Initializing Sender...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    }

    public void send(String message) throws IOException {
        logger.debug("> Sending message '" + message);
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        //logger.debug("> Message sent.");
    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
        logger.debug("> Sender Closed.");
    }

}
