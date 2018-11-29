package com.kafka;

/**
 * 消费者运行入口
 *
 * @author lvyc7
 * @date 2018-11-29 17:12
 */
public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "10.161.11.156:6667 ,10.161.11.157:6667 ,10.161.11.158:6667 ,10.161.11.159:6667 ,10.161.11.160:6667 ,10.161.11.161:6667 ,10.161.11.162:6667 ,10.161.11.163:6667 ,10.161.11.164:6667 ,10.161.11.165:6667 ,10.161.11.166:6667 ,10.161.11.167:6667 ,10.161.11.168:6667 ,10.161.11.169:6667 ,10.161.11.170:6667";
        String topic = "CdrNormal";
        String groupId = "test";
        int consumerNum = 2;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}
