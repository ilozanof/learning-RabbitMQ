package com.ilozanof.learning.rabbitMQ.publishSusbscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSender {
    private Logger logger = LoggerFactory.getLogger(LogSender.class.getName());

    private final String    EXCHANGE_NAME = "logs";

    private Connection      connection;
    private Channel         channel;

    public LogSender() throws Exception {
        logger.debug("> Initializing Log Sender...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    }

    public void sendLog(String msg) throws Exception {
        logger.debug("> Sending log '" + msg + "'");
        channel.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes());
    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
        logger.debug("> Sender Closed.");
    }
}
