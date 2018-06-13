package com.ilozanof.learning.rabbitMQ.workQueues;


import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Random;

public class Worker {
    // Logger...
    private final Logger logger = LoggerFactory.getLogger(Worker.class.getName());

    // RabbitMQ objects...
    private final String QUEUE_NAME = "hello";
    private Channel     channel;

    // Worker info...
    private String      workerId;
    private boolean     isWorking           = false;    // is it Processing the task? ...
    private boolean     done                = false;    // Has it finished processing tht ask?
    private int         numTasksProcessed   = 0;        // Num tasks processed successfully
    private boolean     autoAck             = false;    // Auto message acknowledgment
    private int         failProbability     = 0;        // probability of the Worker fail¡ing while
                                                        // processing the task..

    /**
     * worker Constructor
     *
     * @param workerId          worker Id (for logging)
     * @param autoAck           Auto message-acknowledge
     * @param failProbability   Probability (1-1009 of failing while processing the task
     * @throws Exception
     */
    public Worker(String workerId, boolean autoAck, int failProbability) throws Exception {
        logger.debug("> Worker " + workerId + ": Initializing Worker...");

        this.workerId = workerId;
        this.autoAck = autoAck;
        this.failProbability = failProbability;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    }

    /**
     * Connects the Woprker with the RabbitMQ queue and assign a consumer to it...
     *
     * @throws Exception
     */
    public void receive() throws Exception {

        // We create the Consumer and add a new "doWork" method, which waits 1
        // second for each dot (.) the message contains, to simulate some
        // work processing...

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                try {
                    init();
                    String message = new String(body, "UTF-8");

                    logger.debug("> Worker " + workerId + " > Working on Message '" + message + "'");
                    doWork(message);
                    logger.debug("> Worker " + workerId + " > message processed.");

                    // If applies, we acknowledge the task being done...
                    if (!autoAck) channel.basicAck(envelope.getDeliveryTag(), false);

                } catch (Exception e) {
                    logger.debug(e.getMessage());
                    throw new IOException(e.getMessage());
                } finally {
                    isWorking = false;
                }
            }
        };

        // We connect to the Queue and consume the task...
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

    }

    /**
     * Initialize this worker. this initialization code goes here instead of the Constructor, because the
     * Worker is only instantiated once, but we need this variables to be initialized FOR EACH task.
     */
    public void init() {
        done = false;
        isWorking = false;
    }

    /**
     * Implements the work being done, or the task being performed. In order to simulate some
     * heavy processing, we use Thread.sleep(). fore ach dot (.) in the message, we wait one
     * second.
     * Depending on the value of "failProbability" set in the Constructor, this method might
     * ort might not throw an Exception.
     *
     * @param msg               text Message to be processed
     * @throws Exception
     */
    public void doWork(String msg) throws Exception {

        // Depending on the probability, we force a quick exit...
        int number = new Random().nextInt(100) + 1;
        if (number <= failProbability)
            throw new Exception("> Worker " + workerId + " > FORCING EXIT!!!");

        // We process the Message...
        isWorking = true;
        for (char ch : msg.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }

        // We update some variables...
        isWorking = false;
        done = true;
        numTasksProcessed++;
    }

    /**
     * Close the channel and fre resources..
     * @throws Exception
     */
    public void close() throws Exception {
        if (channel != null) channel.close();
    }

    // getter
    public boolean isWorking() {
        return isWorking;
    }

    // getter
    public boolean isDone() {
        return done;
    }

    // getter
    public String getWorkerId() {
        return workerId;
    }

    // getter
    public int getNumTasksProcessed() {
        return numTasksProcessed;
    }
}
