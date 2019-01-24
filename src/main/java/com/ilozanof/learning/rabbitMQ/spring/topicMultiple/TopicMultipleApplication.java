package com.ilozanof.learning.rabbitMQ.spring.topicMultiple;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class TopicMultipleApplication {

    static final String topicQueueImportant = "importantQueue";
    static final String topicQueueError = "errorQueue";
    static final String topicExchangeName = "topicExchange";

    @Bean
    List<Declarable> topicBindings() {
        Queue topicQueue1 = new Queue(topicQueueImportant, false);
        Queue topicQueue2 = new Queue(topicQueueError, false);

        TopicExchange topicExchange = new TopicExchange(topicExchangeName);

        return Arrays.asList(
                topicQueue1,
                topicQueue2,
                topicExchange,
                BindingBuilder.bind(topicQueue1).to(topicExchange).with("#.important"),
                BindingBuilder.bind(topicQueue2).to(topicExchange).with("#.error")
        );
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory());
        factory.setConcurrentConsumers(3);
        factory.setMaxConcurrentConsumers(4);
        return factory;
    }


    public static void main(String ...args) throws InterruptedException {
        SpringApplication.run(TopicMultipleApplication.class, args).close();
    }
}
