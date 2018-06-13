package com.ilozanof.learning.rabbitMQ.headers;

import com.ilozanof.learning.rabbitMQ.common.SignalTest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeadersTest extends SignalTest {
    private Logger logger = LoggerFactory.getLogger(HeadersTest.class.getName());

    @Before
    public void init() throws  Exception {
        this.receiver = new SignalReceiverHeaders();
        this.sender = new SignalSenderHeaders();
    }

    @Test
    public void testTopic() throws Exception {
        super.testSignals();

    }
}