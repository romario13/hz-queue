package ru.romario.hzqueue.oom;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Partition;

import java.util.logging.Logger;

/**
 */
public class Test5 {

    public static final Logger logger = Logger.getLogger(Test5.class.getName());

    public static final String QNAME = "testQ";

    public static void main(String[] args) {

        try {
            run();
        } finally {
            Hazelcast.shutdownAll();
        }
    }


    private static void run() {

        HazelcastInstance hzInstance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hzInstance2 = Hazelcast.newHazelcastInstance();

        String hz1PartitionKey = hzInstance1.getPartitionService()
                .randomPartitionKey();
        String queueName = QNAME + "@" + hz1PartitionKey;

        // who is queue owner?

        HazelcastInstance ownerInstance;
        HazelcastInstance secondInstance;

        Partition partition = hzInstance1.getPartitionService().getPartition(hz1PartitionKey);
        if (hzInstance1.getCluster().getLocalMember().equals(partition.getOwner())) {
            ownerInstance = hzInstance1;
            secondInstance = hzInstance2;
        } else {
            ownerInstance = hzInstance2;
            secondInstance = hzInstance1;
        }

        IQueue<Integer> queue = ownerInstance.getQueue(queueName);

        long startTime = System.currentTimeMillis();

        int i = 0;
        while (i++ < 100000) {
            if (i % 10000 == 0) {
                logger.info("add " + Integer.toString(i) + "\t" +
                        String.format("%8.3f", (double) (System.currentTimeMillis() -
                                startTime) / i));
            }
            queue.add(i);
        }

        ownerInstance.shutdown();
        queue = secondInstance.getQueue(queueName);

        startTime = System.currentTimeMillis();

        i = 0;
        while (i++ < 100000) {
            if (i % 10000 == 0) {
                logger.info("poll " + Integer.toString(i) + "\t"
                        + String.format("%8.3f", (double) (System.currentTimeMillis() -
                        startTime) / i));
            }
            Integer intVal = queue.poll();
            if (intVal == null || intVal.intValue() != i) {
                logger.info("Error: " + (intVal) + "!=" + i);
                System.exit(-1);
            }
        }

    }
}
