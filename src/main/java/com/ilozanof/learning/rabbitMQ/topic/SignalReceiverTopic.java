package com.ilozanof.learning.rabbitMQ.topic;

import com.ilozanof.learning.rabbitMQ.common.SignalReceiver;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalReceiverTopic implements SignalReceiver  {

    private Logger logger = LoggerFactory.getLogger(SignalReceiverTopic.class.getName());

    private final String EXCHANGE_NAME = "topicExchange";
    private final String QUEUE_NAMES[] = {"dev", "test", "prod"};
    private Connection connection;
    private Channel channel;

    public SignalReceiverTopic() throws Exception {
        logger.debug("> Initializing Log Consumer...");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        // We set up the Exchange..
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // We set up the Queues, and the binding to the exchange...
        // Basically, we use the name of the Queue as a pattern, so:
        //   - if the routing key is "xxxx.dev", then the message ios sent to the
        //     "dev" queue, and the same goes for the others.

        for (int i = 0; i < QUEUE_NAMES.length; i++) {
            channel.queueDeclare(QUEUE_NAMES[i], false, false, true, null);
            String routingKey = "#." + QUEUE_NAMES[i];
            channel.queueBind(QUEUE_NAMES[i], EXCHANGE_NAME, routingKey);
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
