package ru.romario.hzqueue.oom;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Partition;

import java.util.logging.Logger;

/**
 */
public class Test6 {

    public static final Logger logger = Logger.getLogger(Test6.class.getName());

    public static final String QNAME = "testQ";

    public static void main(String[] args) {

        try {
            run();
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static Config createConfig(String name) {
        Config config = new Config(name);

        QueueConfig queueConfig = config.getQueueConfig(QNAME);
        queueConfig.setBackupCount(0);

        return config;
    }

    private static void run() {

        HazelcastInstance hzInstance1 = Hazelcast.newHazelcastInstance(createConfig("1"));
        HazelcastInstance hzInstance2 = Hazelcast.newHazelcastInstance(createConfig("2"));

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
                logger.info("add " + Integer.toString(i) + "\t" + String.format("%8.3f",
                        (double) (System.currentTimeMillis() -
                                startTime) / i));
            }
            queue.add(i);
        }


        ownerInstance.shutdown();
        queue = secondInstance.getQueue(queueName);

        Integer intVal = queue.poll();

        if (intVal != null) {
            logger.info("Error: Queue should be empty");
            System.exit(-1);
        }


    }
}
