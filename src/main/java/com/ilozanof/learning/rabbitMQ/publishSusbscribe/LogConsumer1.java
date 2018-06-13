package com.ilozanof.learning.rabbitMQ.publishSusbscribe;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogConsumer1 extends DefaultConsumer {

    private Logger logger = LoggerFactory.getLogger(LogConsumer1.class.getName());

    public LogConsumer1(Channel channel) {
        super(channel);
        logger.debug("> LogConsumer1 initialized.");
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        logger.debug("> LogConsumer1 - processing log '" +  msg + "'...");
    }
}
