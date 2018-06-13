package com.ilozanof.learning.rabbitMQ.publishSubscribe;

import com.ilozanof.learning.rabbitMQ.publishSusbscribe.LogReceiver;
import com.ilozanof.learning.rabbitMQ.publishSusbscribe.LogSender;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublishSubscribeTest {

    private Logger logger = LoggerFactory.getLogger(PublishSubscribeTest.class.getName());

    @Before
    public void init() {}

    @Test
    public void testingPSLogs() throws Exception {
        final int NUM_LOGS = 5;

        // We set up the Consumers...
        LogReceiver logReceiver = new LogReceiver();
        logReceiver.setupConsumers();

        // We set up the LogSender and start sending logs like crazy.....
        LogSender logSender = new LogSender();
        for (int i = 0; i < NUM_LOGS; i++) logSender.sendLog("dummyLog-" + i);

        logSender.close();
        logReceiver.close();
    }
}
