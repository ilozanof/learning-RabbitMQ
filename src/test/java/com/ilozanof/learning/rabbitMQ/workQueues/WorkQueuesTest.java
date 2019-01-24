package com.ilozanof.learning.rabbitMQ.workQueues;

import com.ilozanof.learning.rabbitMQ.helloWorld.Sender;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WorkQueuesTest {

    Logger logger = LoggerFactory.getLogger(WorkQueuesTest.class.getName());

    @Before
    public void init() {}

    // Convenience method to create a list of Workers, put them to wait for tasks, and return the list
    private List<Worker> getWorkers(int numWorkers, boolean autoAck, int failProbability) throws Exception {
        List<Worker> result = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Worker worker = new Worker("w" + i, autoAck, failProbability);
            worker.receive();
            result.add(worker);
        }
        return result;
    }

    // Waits for all the workers in the list to finish their tasks...
    private void waitForWorkers(List<Worker> workers) throws Exception {
        boolean allFinished = false;
        do {
            allFinished = true;
            Thread.sleep(100);
            for (Worker worker : workers) {
                allFinished = allFinished && !worker.isWorking();
            }
        } while (!allFinished);
    }


    // Convenience Method.
    // It sends an specified number of messages to the Queue, to be processed later by Workers..
    public void runSender(int numTasks) throws Exception {
        Sender sender = null;
        try {
            // We send some tasks to the Queue...
            sender = new Sender();
            String dots = "";
            for (int i = 0; i < numTasks; i++) {
                dots += ".";
                String message = "Message" + dots;
                sender.send(message);
            }
        } finally {
            // We close resources...
            try {sender.close();} catch (Exception e) {}
        }
    }

    // convenience method...
    public void runSendersAndWorkers(int numTasks, int numWorkers, boolean autoAck, int failProbability) {

        List<Worker> workers = null;
        Sender sender = null;
        try {
            int numFinishedTasks = numTasks;



            // We set up a set of workers, as many as tasks left...
            logger.debug("- Setting up " + numWorkers + " workers...");
            workers = getWorkers(numWorkers, false, failProbability);

            // This time, we first send the messages to the Queue...
            this.runSender(numTasks);

            do {
                // We wait for all of them to finish...
                logger.debug("Waiting for workers to finish...");
                waitForWorkers(workers);

                // We check how many tasks are left to do...
                numFinishedTasks = (int) workers.stream().collect(Collectors.summingInt(w -> w.getNumTasksProcessed()));
            } while (numFinishedTasks < numTasks);

        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            // We close resources...
            workers.forEach(w -> {try {w.close();} catch (Exception e) {}});
        }
    }

    /**
     * Testing a simple scenario, running several workers and sending several messages. the workers are
     * working in parallel, so at some time they might be running all of them at the same time, processing
     * the tasks. It ends when all of them finish.
     * All the workers do NOT acknowledge the tasks, and all of them work fine (there is no simulation of failiure)
     */
    @Test
    public void testRoundRobin() {
        final int NUM_TASKS = 10;
        final int NUM_WORKERS = 5;

        try {
            runSendersAndWorkers(NUM_TASKS, NUM_WORKERS, false, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Testing different Workers. This time, we are simulating that some of them finish unexpectedtly, so
     * we can check how RabbitMQ reassign the Task to other worker.
     *
     * We are creating N Tasks, and M workers (more Tasks than workers)m, and we wait until all the tasks
     * are done.
     */
    @Test
    public void testFailing() {
        final int NUM_TASKS = 10;
        final int NUM_WORKERS = 3;

        try {
            runSendersAndWorkers(NUM_TASKS, NUM_WORKERS, true, 40);
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

}
