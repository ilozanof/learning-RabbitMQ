package com.ilozanof.learning.rabbitMQ.headers;

import com.ilozanof.learning.rabbitMQ.common.SignalReceiver;
import com.ilozanof.learning.rabbitMQ.topic.SignalConsumer;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SignalReceiverHeaders implements SignalReceiver {

    private Logger logger = LoggerFactory.getLogger(SignalReceiverHeaders.class.getName());

    private final String EXCHANGE_NAME = "headerExchange";
    private final String QUEUE_NAMES[] = {"dev", "test", "prod"};
    private Connection connection;
    private Channel channel;

    public SignalReceiverHeaders() throws Exception {
        logger.debug("> Initializing Log Consumer...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        // We set up the Exchange..
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

        // We set up the Queues, and the binding to the exchange...
        // We do the mapping based on the headers. Here, the SYSTEM does
        // NOT matter, only the Environment...
        // In this case, the Queue NAME represents THE ENVIRONMENT, so
        // we use the Queue name as the VALUE in the Header...


        for (int i = 0; i < QUEUE_NAMES.length; i++) {

            // We declare the Queue and the Headers...
            channel.queueDeclare(QUEUE_NAMES[i], false, false, true, null);
            Map<String, Object> headers = new HashMap<>();
            headers.put("env", QUEUE_NAMES[i]);

            //We do the binding...
            channel.queueBind(QUEUE_NAMES[i], EXCHANGE_NAME, "", headers);
        }
    }

    @Override
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

    @Override
    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection!= null) connection.close();
    }
}
