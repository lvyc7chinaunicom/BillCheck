package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * test1
 *
 * @author lvyc7
 * @date 2018-11-28 23:20
 */
public class test1 {

    String topic = "CdrNormal";

    public static void main(String[] args) {
        new test1().run();
    }

    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.161.11.156:6667,10.161.11.157:6667,10.161.11.158:6667,10.161.11.159:6667,10.161.11.160:6667,10.161.11.161:6667,10.161.11.162:6667,10.161.11.163:6667,10.161.11.164:6667,10.161.11.165:6667,10.161.11.166:6667,10.161.11.167:6667,10.161.11.168:6667,10.161.11.169:6667,10.161.11.170:6667");
        props.put("group.id", topic + "_test");
        /* 关闭自动确认选项 */
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("hello a ");
        try {
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                consumer.listTopics();
//                System.out.println("***********");
//                System.out.println(records.count());
//                for (ConsumerRecord<String, String> record : records) {
//                    buffer.add(record);
//                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
//                            record.topic(),
//                            record.partition(),
//                            record.offset(),
//                            record.key(),
//                            record.value()));
//                }
//                if (buffer.size() >= minBatchSize) {
//                    System.out.println(buffer.size());
//                    consumer.commitSync();
//                    buffer.clear();
//                }
            }
        } finally {
            consumer.close();
        }
    }

}
