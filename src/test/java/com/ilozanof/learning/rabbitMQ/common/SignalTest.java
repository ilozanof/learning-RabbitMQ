package com.ilozanof.learning.rabbitMQ.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SignalTest {
    private Logger logger = LoggerFactory.getLogger(com.ilozanof.learning.rabbitMQ.topic.TopicTest.class.getName());

    protected SignalReceiver  receiver;
    protected SignalSender    sender;

    public void testSignals() throws Exception {

        receiver.setUpConsumers();

        // These routing Keys represent some systems and environments:
        final String systems[] = {  "System1", "System2",
                "System1.subsystemA",
                "System1.subsystemB"
        };

        final String envs[] = {     "test", "dev", "prod"};

        // We send 1 signal to each one of the previous systems and environments...
        for (int i = 0; i < systems.length; i++) {
            for (int b = 0; b < envs.length; b++) {
                String signal = "signal to " + systems[i] + "-" + envs[b];
                sender.send(signal, systems[i], envs[b]);
            }
        }

        // WE wait for some time, to let all the consumers to finish...
        Thread.sleep(1000);

        // WE close resources...
        sender.close();
        receiver.close();
    }
}
