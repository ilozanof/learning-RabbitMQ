package com.ilozanof.learning.rabbitMQ.spring.scalable;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import static com.google.common.base.Preconditions.*;

/**
 * This class represents a SCalabel Container: A component in RabbitMQ that listens to the messages sent to one
 * specific Queue, and takes care of executing the Consumers in order to consume that message.
 * Some characteristicis of this Container:
 * - It delegates its tasks to the "SimpleMessageListenerContainer", which is the basic implementation of the same
 * component provided by Rabbit. Additional features are added by this class, on top of it.
 * - It only works on 1 Queue, as a restriction over SimpleMessageListenerContainer, which accepts more than one.
 * - It one works with 1 type of Consumer, which must implement the "Consumer" interface.
 * THE MAIN CLASS OF THIS CLASS IS:
 * - It can scale up or down, adding or removing more Threads, in order to support more or less consumers running
 * at the same time.
 */

public class ScalableContainer {
    private String name;
    private ConnectionFactory connectionFactory;
    private SimpleMessageListenerContainer listenerContainer;
    private Queue queue;
    private Consumer consumer;
    private int concurrentConsumers = 0;
    private int maxConcurrentConsumers = 0;

    // Constructor
    public ScalableContainer(String name, ConnectionFactory connectionFactory, Queue queue, Consumer consumer,
                             int maxConcurrentConsumers) {
        System.out.println(">> Creating Scalable Container...");
        // We set the properties...
        this.name = name;
        this.connectionFactory = connectionFactory;
        this.queue = queue;
        this.consumer = consumer;
        this.maxConcurrentConsumers = maxConcurrentConsumers;
        this.concurrentConsumers = 1; // We start with only 1 Thread...

        // We initialize the consumer...
        MessageListenerAdapter listenerAdapter = new MessageListenerAdapter(consumer, "consume");

        // We create the message listener and do the binding...
        listenerContainer = new SimpleMessageListenerContainer();
        listenerContainer.setConnectionFactory(connectionFactory);
        listenerContainer.setQueueNames(queue.getName());
        listenerContainer.setMessageListener(listenerAdapter);

        System.out.println(">> Scalable Container created.");
    }

    // It starts the Container.
    public void start() {
        System.out.println(">> Starting scalable Container...");
        if (concurrentConsumers > 0 ) {
            listenerContainer.afterPropertiesSet();
            listenerContainer.start();
            System.out.println(">> Scalable Container started.");
        }
    }

    // It stops the container.
    public void stop() {
        System.out.println(">> Stopping scalable Container...");
        // We stop the listenerContainer in a separate Thread, so we don't get stuck waiting for it to stop...
        listenerContainer.stop();
        System.out.println(">> Scalable Container stopped.");
        listenerContainer.shutdown();
        System.out.println(">> Scalable Container Shutdown.");

        /*
            NOTE: We used first a Thread to stop the container, so we didn't have to wait for it. But with the
            code below, the App used to hang,, since there seems to be a thread loose somewhere.
         */

        /*
        Executors.newSingleThreadExecutor().submit(() -> {
            listenerContainer.stop();
            System.out.println(">> Scalable Container stopped.");
            listenerContainer.shutdown();
            System.out.println(">> Scalable Container Shutdown.");
        });
        */

    }

    // It scales to the number of Threads specified. If the specified number is ZERO, we stop the container
    public void scale(int concurrentConsumers) {
        checkArgument(concurrentConsumers > 0, "CncurrentConsumers must be at least 1.");
        System.out.println(">> Scaling Container to " + concurrentConsumers + " concurrent consumers...");
        if (this.concurrentConsumers != concurrentConsumers) {
            if (!listenerContainer.isActive()) System.out.println(">> Listener NOT ACTIVE! How Come??????");
            listenerContainer.setConcurrentConsumers(concurrentConsumers);
            this.concurrentConsumers = concurrentConsumers;
        }
    }

}
