package com.kafka;

import java.util.ArrayList;
import java.util.List;

/**
 * 消费者组
 *
 * @author lvyc7
 * @date 2018-11-29 17:11
 */
public class ConsumerGroup {
    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; ++i) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }
}
