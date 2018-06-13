package com.ilozanof.learning.rabbitMQ.helloWorld;

import org.junit.Before;
import org.junit.Test;

public class HelloWorldTest {

    @Before
    public void init() {}

    @Test
    public void testSenderAndReceiver() throws Exception {
        Sender sender = new Sender();
        Receiver receiver = new Receiver();

        sender.send("Hello World, I'm using RabbitMQ!");
        receiver.receive();

        sender.close();
        receiver.close();
    }
}
