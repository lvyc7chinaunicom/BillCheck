package com.kafka;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * 获取kafka集群borkers列表和对应的端口号
 *
 * @author lvyc7
 * @date 2018-11-28 10:09
 */
public class GetBrokers {

    private static final Logger logger = Logger.getLogger(GetBrokers.class);


    public static void main(String[] args) {
        new GetBrokers().test();
    }

    public void test() {
        //消费到的msg列表
        ArrayList<String> result = new ArrayList<>();
        Map<String, List<String>> yuToBrokers = new HashMap<>();
        yuToBrokers.put("2-4", Arrays.asList("10.161.11.156", "10.161.11.157", "10.161.11.158", "10.161.11.159", "10.161.11.160", "10.161.11.161", "10.161.11.162", "10.161.11.163", "10.161.11.164", "10.161.11.165", "10.161.11.166", "10.161.11.167", "10.161.11.168", "10.161.11.169", "10.161.11.170"));
        List<Integer> ports = new ArrayList<>(Arrays.asList(6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667));

        long maxReads = 10;
        long targetOffset = -1;
        String topic = "CdrNormal";
        int partition = 0;

        KafkaParameterBean bean = new KafkaParameterBean(maxReads, topic, partition, yuToBrokers.get("2-4"), ports);
        try {
            for (int i=0;i<3;i++){
                KafkaUtil kafkaUtil = new KafkaUtil(targetOffset);
                result = kafkaUtil.getMsg(bean);
                targetOffset = kafkaUtil.getTargetOffset() + maxReads;
                System.out.println(kafkaUtil.getTargetOffset());
                if (result.size() != 0) {
                    result.stream().forEach(System.out::println);
                } else {
                    System.out.println("没有获得到消息。");
                }
                System.out.println("#########################");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
