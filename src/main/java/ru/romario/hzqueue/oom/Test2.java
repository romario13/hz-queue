package ru.romario.hzqueue.oom;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

import java.util.logging.Logger;

/**
 */
public class Test2 {

    public static final Logger logger = Logger.getLogger(Test2.class.getName());

    public static final String QNAME = "testQ";

    public static void main(String[] args) {

        try {
            run();
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static void run() {

        Config config = new Config("queueTest");

        QueueConfig queueConfig = config.getQueueConfig(QNAME);

        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(true);
        queueStoreConfig.setStoreImplementation(new MockQueueStore());
        queueStoreConfig.getProperties().setProperty("memory-limit", "0");

        queueConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance(config);

        IQueue<Integer> queue = hzInstance.getQueue(QNAME);

        long startTime = System.currentTimeMillis();

        int i = 0;
        while (i++ < 2000000) {
            if (i % 10000 == 0) {
                logger.info(Integer.toString(i) + "\t" + String.format("%8.3f", (double) (System.currentTimeMillis() -
                        startTime) / i));
            }
            queue.add(i);
        }
    }

}
