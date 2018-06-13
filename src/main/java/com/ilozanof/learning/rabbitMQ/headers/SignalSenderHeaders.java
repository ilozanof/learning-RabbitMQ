package com.ilozanof.learning.rabbitMQ.headers;

import com.ilozanof.learning.rabbitMQ.common.SignalSender;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SignalSenderHeaders implements SignalSender  {
    private Logger logger = LoggerFactory.getLogger(SignalSenderHeaders.class.getName());

    private final String EXCHANGE_NAME = "headerExchange";

    private Connection      connection;
    private Channel         channel;

    public SignalSenderHeaders() throws Exception {
        logger.debug("> Initializing Signal Sender...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
    }

    @Override
    public void send(String msg, String system, String env) throws Exception {
        logger.debug("> Sending '" + msg + "'");
        Map<String, Object> headers = new HashMap<>();
        headers.put("system", system);
        headers.put("env", env);

        // The routingKey is NOT used for HEADER exchanges...

        channel.basicPublish( EXCHANGE_NAME,
                    "",
                              new AMQP.BasicProperties.Builder().headers(headers).build(),
                              msg.getBytes());
    }

    @Override
    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
        logger.debug("> Sender Closed.");
    }
}
