package com.ilozanof.learning.rabbitMQ.spring.scalable;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;


/**
 * A Spring-Boot application. It starts up the Message container listeners and all the components
 * (Queues, etc). In this ap we are using the "ScalableContainer" component, which is a wrapper
 * over the "SimpleMessageListenerContainer", so some things are quite different:
 *
 * - The "ScalableComponent" is NOT managed by the Spring lifecycle, since we don't return an instance
 * in a @Bean method instead we return a List of them. So we need to start them manually. this is done
 * in a @PostConstruct method.
 *
 * - For the same reason, the ScalableContainer must be closed and destroyed properly when the Spring context
 * is gone. This is done in a @PreDestroy method.
 *
 * -
 */
@SpringBootApplication
public class TopicScalableApplication {
    static final String topicExchangeName = "spring-boot-topicExchange";
    static final String topicQueueName = "spring-boot-topicQueue";

    @Autowired
    ConnectionFactory connectionFactory;

    @Bean
    Queue queue() {
        return new Queue(topicQueueName, false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange(topicExchangeName);
    }

    @Bean
    Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("foo.bar.#");
    }

    @Bean
    Consumer consumer() {
        return new Receiver();
    }

    @Bean
    List<ScalableContainer> scalableContainers(ConnectionFactory connectionFactory) {
        List<ScalableContainer> result = new ArrayList<>();
        result.add(new ScalableContainer("scalableContainer1", connectionFactory,
                queue(), consumer(), 10));
        return result;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startListeners() throws Exception {
        for (ScalableContainer container: scalableContainers(connectionFactory)) {
            container.start();
        }
    }

    @PreDestroy
    public void stopListeners() throws Exception {
        for (ScalableContainer container: scalableContainers(connectionFactory)) {
            container.stop();
        }
        System.out.println("Pre-Destroy method called.");
    }

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(TopicScalableApplication.class, args).close();
    }

}
