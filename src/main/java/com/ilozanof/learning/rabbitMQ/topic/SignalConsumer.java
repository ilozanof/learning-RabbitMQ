package com.ilozanof.learning.rabbitMQ.topic;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SignalConsumer extends DefaultConsumer {

    private Logger logger = LoggerFactory.getLogger(SignalConsumer.class.getName());

    private String consumerId;

    public SignalConsumer(String consumerId, Channel channel) {
        super(channel);
        this.consumerId = consumerId;
        logger.debug("> " + consumerId + " initialized.");
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        logger.debug("> " + consumerId + " - processing signal '" +  msg + "'...");
    }
}
