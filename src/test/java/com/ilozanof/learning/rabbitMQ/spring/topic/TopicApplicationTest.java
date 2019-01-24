package com.ilozanof.learning.rabbitMQ.spring.topic;

import org.junit.Test;

public class TopicApplicationTest {

    @Test
    public void test() {
        try {
            TopicApplication.main(new String[]{});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
