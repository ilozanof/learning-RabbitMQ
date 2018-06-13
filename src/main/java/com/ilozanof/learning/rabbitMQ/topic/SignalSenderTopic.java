package com.ilozanof.learning.rabbitMQ.topic;

import com.ilozanof.learning.rabbitMQ.common.SignalSender;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalSenderTopic implements SignalSender  {
    private Logger logger = LoggerFactory.getLogger(SignalSenderTopic.class.getName());

    private final String EXCHANGE_NAME = "topicExchange";

    private Connection      connection;
    private Channel         channel;

    public SignalSenderTopic() throws Exception {
        logger.debug("> Initializing Signal Sender...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
    }

    @Override
    public void send(String msg, String system, String env) throws Exception {
        logger.debug("> Sending Signal '" + msg + "'");
        String routingKey = system + "." + env;
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, msg.getBytes());
    }

    @Override
    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
        logger.debug("> Sender Closed.");
    }
}
