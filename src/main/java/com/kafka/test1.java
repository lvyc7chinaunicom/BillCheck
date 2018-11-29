package com.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.logging.Logger;

/**
 * test1
 *
 * @author lvyc7
 * @date 2018-11-28 23:20
 */
public class test1 {

    String topic = "CdrFirstFilter";

    public static void main(String[] args) {
        new test1().run();
    }

    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.161.11.156:6667 ,10.161.11.157:6667 ,10.161.11.158:6667 ,10.161.11.159:6667 ,10.161.11.160:6667 ,10.161.11.161:6667 ,10.161.11.162:6667 ,10.161.11.163:6667 ,10.161.11.164:6667 ,10.161.11.165:6667 ,10.161.11.166:6667 ,10.161.11.167:6667 ,10.161.11.168:6667 ,10.161.11.169:6667 ,10.161.11.170:6667");
        props.put("group.id", topic + "_test");
        /* 关闭自动确认选项 */
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.offset", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            for (int i = 0; i < 100; i++) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()));
                    //
                    //数据处理逻辑。 TODO: 2018/11/29 0029
                    //

                }
                //异步提交偏移量offset，提交偏移失败，记录失败的偏移。
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            System.out.println("");
                        }
                    }
                });
            }
        } catch (Exception e) {
            System.out.println("Unexpect error." + e.getMessage());
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

}
