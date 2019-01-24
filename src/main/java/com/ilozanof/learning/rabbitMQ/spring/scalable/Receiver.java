package com.ilozanof.learning.rabbitMQ.spring.scalable;


import org.springframework.stereotype.Component;

/**
 * A basic Queue Consumer
 */
@Component
public class Receiver implements Consumer{

    public void consume(String message) {
        try {
            //System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() + ": Received < " + message + ">");
            Thread.sleep(100);
            System.out.println(this.hashCode() + ":" + Thread.currentThread().getId() + ": Message < " + message + "> processed.");
            } catch (Exception e) {e.printStackTrace();}
    }

}
