package com.ilozanof.learning.rabbitMQ.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalSender {
    private Logger logger = LoggerFactory.getLogger(SignalSender.class.getName());

    private final String EXCHANGE_NAME = "topicExchange";

    private Connection      connection;
    private Channel         channel;

    public SignalSender() throws Exception {
        logger.debug("> Initializing Log Sender...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
    }

    public void sendLog(String msg, String system) throws Exception {
        logger.debug("> Sending log '" + msg + "'");
        channel.basicPublish(EXCHANGE_NAME, system, null, msg.getBytes());
    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
        logger.debug("> Sender Closed.");
    }
}
