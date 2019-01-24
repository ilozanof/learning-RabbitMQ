package com.ilozanof.learning.rabbitMQ.spring.scalable;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class will execute right after the Spring.boot application runs. We execute some initialization code here, in this
 * case we are going to send some messages to the Queue, so we can check the Consumers are triggered properly.
 * In tis case we are performing 2 tasks (in the "run" method):
 *  - We send several sets of messages, in a series of tme intervals (for example, 1 messages every 50 milisecs)
 *  - In parallel, we check every second if we need to scale the Container up or down. The rules for scaling are:
 *     - If we already sent more than "X" messages, we scale the container up to 5 threads.
 *     - After the continer as been scaled up, if we sent "Y" more messages, then we scale it down to 1 Thread.
 *    (The variables "X" and "Y" are the Threshold defined below for the scaling)
 *
 */
@Component
public class Runner implements CommandLineRunner {

    @Autowired
    private List<ScalableContainer> containers;

    private final RabbitTemplate rabbitTemplate;

    // Variables to control the Scaling:
    private AtomicInteger numMsgs = new AtomicInteger(0);
    private AtomicBoolean scaledUp = new AtomicBoolean(false);
    private AtomicBoolean scaledDown = new AtomicBoolean(false);
    private final int SCALE_THRESHOLD_BOTTOM = 5;   // After this, we scale up
    private final int SCALE_THRESHOLD_TOP = 10;     // After this, we scale down

    // Constructor
    public Runner(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    // It sends a message, as many times as specified in the parameer given
    private void sendMessages(String message, int times)  {
        for (int i = 0; i < times; i++) {
            System.out.println("Sending message...");
            rabbitTemplate.convertAndSend(TopicScalableApplication.topicExchangeName, "foo.bar.baz", "Hello from RAbbitMQ!");
            numMsgs.incrementAndGet();
        }
    }

    // It checks if we need to scale the containers up or down, and does it if needed.
    private void checkAndScale() {
        System.out.println("Checking for Scalability (" + numMsgs + " msgs sent so far)...");
        if (!scaledUp.get() && numMsgs.get() >= SCALE_THRESHOLD_BOTTOM) {
            containers.forEach(c -> c.scale(5));
            scaledUp.set(true);
        } else if (!scaledDown.get() && numMsgs.get() >= SCALE_THRESHOLD_TOP) {
            containers.forEach(c -> c.scale(5));
            scaledDown.set(true);
        }
    }

    /**
     * This methods runs after the Spring-boot application starts up. We eecute 2 different Tasks, in 2 different Threads:
     * - In one threads, we send messages to the Queue every second
     * - In another Threads, we check every second if we need tos cale up or down.
     * @param args
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(() -> {this.sendMessages("Hello RabbitMQ!", 1);}, 0, 50, TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(() -> {this.checkAndScale();}, 1000, 1000, TimeUnit.MILLISECONDS);

        executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
        executor.shutdown();
    }
}
