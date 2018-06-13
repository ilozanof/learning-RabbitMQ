package com.ilozanof.learning.rabbitMQ.topic;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicTest {
    private Logger logger = LoggerFactory.getLogger(TopicTest.class.getName());

    @Before
    public void init() {}

    @Test
    public void testTopic() throws Exception {

        // WE set up the Receiver, queues and the exchange...
        SignalReceiver signalReceiver = new SignalReceiver();
        signalReceiver.setUpConsumers();

        // WE set up the Sender and the systems we are sending signals to:
        SignalSender signalSender = new SignalSender();

        // These routing Keys represent some systems and environments:
        final String systems[] = {  "System1.dev", "System2.dev",
                                    "System1.subsystemA.test", "System1.subsystemB.test",
                                    "System1.prod", "System2.prod", "System3.prod"};

        // we send 1 signal to each one of the previous systems...
        for (int i = 0; i < systems.length; i++) {
            String signal = "signal to " + systems[i];
            signalSender.sendLog(signal, systems[i]);
        }

        // WE wait for some time, to let all the consumers to finish...
        Thread.sleep(1000);

        // WE close resources...
        signalSender.close();
        signalReceiver.close();
    }
}
