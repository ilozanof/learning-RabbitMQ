package com.ilozanof.learning.rabbitMQ.helloWorld;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Receiver {
    private final Logger logger = LoggerFactory.getLogger(Receiver.class.getName());
    private final static String QUEUE_NAME = "hello";

    private Connection connection;
    private Channel channel;

    public Receiver() throws Exception {
        logger.debug("> Initializing Receiver...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    }

    public void receive() throws Exception {
        logger.debug("> Accesing queue looking for Messages...");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                logger.debug("> Consumer: Message received: '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
    }
}
