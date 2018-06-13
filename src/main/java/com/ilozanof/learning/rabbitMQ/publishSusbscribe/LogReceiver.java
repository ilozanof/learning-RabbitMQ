package com.ilozanof.learning.rabbitMQ.publishSusbscribe;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogReceiver {

    private Logger logger = LoggerFactory.getLogger(LogReceiver.class.getName());

    private final String    EXCHANGE_NAME = "logs";

    private Connection connection;
    private Channel channel;

    public LogReceiver() throws Exception {
        logger.debug("> Initializing Log Consumer...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    }

    public void setupConsumers() throws Exception {

        // We crete and register the different consumers, each one listening to a
        // different queue...

        // Consumer 1:
        Consumer logConsumer1 = new LogConsumer1(channel);
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, EXCHANGE_NAME, "");
        channel.basicConsume(queueName1, true, logConsumer1);

        // Consumer 1:
        Consumer logConsumer2 = new LogConsumer2(channel);
        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, EXCHANGE_NAME, "");
        channel.basicConsume(queueName2, true, logConsumer2);

    }

    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
    }
}
