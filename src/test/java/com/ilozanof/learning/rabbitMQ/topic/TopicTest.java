package com.ilozanof.learning.rabbitMQ.topic;

import com.ilozanof.learning.rabbitMQ.common.SignalTest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicTest extends SignalTest  {
    private Logger logger = LoggerFactory.getLogger(TopicTest.class.getName());

    @Before
    public void init() throws  Exception {
        this.receiver = new SignalReceiverTopic();
        this.sender = new SignalSenderTopic();
    }

    @Test
    public void testTopic() throws Exception {
        super.testSignals();

    }
}
