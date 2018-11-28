package com.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * kafka连接及读取消息工具类
 *
 * @author lvyc7
 * @date 2018-11-28 14:08
 */
public class KafkaUtil {

    private static final Logger logger = Logger.getLogger(KafkaUtil.class);
    private List<String> m_replicaBrokers = new ArrayList<>();
    private List<Integer> m_replicaPorts = new ArrayList<>();
    private long targetOffset;

    public long getTargetOffset() {
        return targetOffset;
    }

    public KafkaUtil(long targetOffset) {
        this.targetOffset = targetOffset;
    }

    public ArrayList<String> getMsg(KafkaParameterBean kafkaParameterBean) throws Exception {
        ArrayList<String> msgList = new ArrayList<>();
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(kafkaParameterBean.getA_seedBrokers(),
                kafkaParameterBean.getA_ports(),
                kafkaParameterBean.getA_topic(),
                kafkaParameterBean.getA_partition());
        if (metadata == null) {
            logger.error("Can't find metadata for Topic and Partition. Exiting");
            return msgList;
        }
        if (metadata.leader() == null) {
            logger.error("Can't find Leader for Topic and Partition. Exiting");
            return msgList;
        }
        String leadBroker = metadata.leader().host();
        int a_port = metadata.leader().port();
        String clientName = "Client_" + kafkaParameterBean.getA_topic() + "_" + kafkaParameterBean.getA_partition();

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 10000, 64 * 1024, clientName);
        // kafka.api.OffsetRequest.EarliestTime() finds the beginning of the data in the logs and starts streaming from there
        long readOffset = getLastOffset(consumer, kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition(), kafka.api.OffsetRequest.EarliestTime(), clientName);
        long latestOffset = getLastOffset(consumer, kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition(), kafka.api.OffsetRequest.LatestTime(), clientName);
        System.out.println(readOffset);
        System.out.println(latestOffset);
        //若存储的偏移大于最新偏移，将要读的偏移置为最新的偏移
        if (this.targetOffset > latestOffset) {
            readOffset = latestOffset;
        } else {
            //若存储的偏移大于最早的偏移，将存储的偏移置为要读取的偏移。否则，使用最早的偏移作为要读取的偏移。
            if (this.targetOffset > readOffset) {
                readOffset = this.targetOffset;
            }
        }
        //将确定好的偏移保存到变量中，之后存储到redis里。
        this.targetOffset = readOffset;
        //System.out.println("offset:" + readOffset);
        int numErrors = 0;
        while (kafkaParameterBean.getA_maxReads() > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 10000, 64 * 1024, clientName);
            }
            // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition(), readOffset, 64 * 10000)
                    .build();

            FetchResponse fetchResponse = consumer.fetch(req);

            //Identify and recover from leader changes
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition());
                StringBuilder errStr = new StringBuilder();
                logger.error(errStr.append("Error fetching data from the Broker:").append(" Reason: ").append(String.valueOf(code)).toString());
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition(), kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                //查找新的leader
                metadata = findNewLeader(leadBroker, kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition(), a_port);
                leadBroker = metadata.leader().host();
                a_port = metadata.leader().port();
                continue;
            }
            numErrors = 0;

            //Fetch the data
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(kafkaParameterBean.getA_topic(), kafkaParameterBean.getA_partition())) {
                if (kafkaParameterBean.getA_maxReads() > 0) {
                    long currentOffset = messageAndOffset.offset();
                    //This is needed since if Kafka is compressing the messages,
                    //the fetch request will return an entire compressed block even if the requested offset isn't the beginning of the compressed block.
                    if (currentOffset < readOffset) {
                        StringBuilder errStr = new StringBuilder();
                        logger.error(errStr.append("Found an old offset: ").append(" Expecting: ").append(String.valueOf(readOffset)).toString());
                        continue;
                    }
                    readOffset = messageAndOffset.nextOffset();
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    //System.out.println("currentOffset: " + currentOffset + " nextOffset: " + readOffset);
                    //System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                    msgList.add(new String(bytes, "UTF-8"));
                    numRead++;
                    kafkaParameterBean.setA_maxReads(kafkaParameterBean.getA_maxReads() - 1);
                }
            }
            //If we didn't read anything on the last request we go to sleep for a second so we aren't hammering Kafka when there is no data.
            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                    //System.out.println(offSetKey + " sleep wait msg");
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) {
            consumer.close();
        }
        return msgList;
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                               long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private PartitionMetadata findNewLeader(String a_oldLeader, String a_topic, int a_partition,
                                            int a_oldLeader_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, m_replicaPorts, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) &&
                    a_oldLeader_port == metadata.leader().port() && i == 0) {
                // first time through if the leader hasn't changed, give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                goToSleep = true;
            } else {
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<String> a_seedBrokers, List<Integer> a_port, String a_topic,
                                         int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (int i = 0; i < a_seedBrokers.size(); i++) {
            String seed = a_seedBrokers.get(i);
            SimpleConsumer consumer = null;
            try {
                //1.构建一个消费者,它是获取元数据的执行者
                consumer = new SimpleConsumer(seed, a_port.get(i), 100000, 1024 * 1024, "MsgCmpFile");
                //2.构造请求topic元数据的request
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                //3.发送请求获取元数据
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                //4.获取主题元数据列表
                List<TopicMetadata> metaData = resp.topicsMetadata();
                //5.提取主题元数据列表中指定分区的元数据信息
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            //System.out.println("equalPart:" + part.toString());
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                StringBuilder errst = new StringBuilder();
                logger.error(errst.append("Error communicating with Broker [").append(seed).append("] to find Leader for [").append(a_topic)
                        .append(", ").append(a_partition).append("] Reason: ").append(e.toString()).toString());
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            m_replicaPorts.clear();
            for (BrokerEndPoint replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
                m_replicaPorts.add(replica.port());
            }
        }
        return returnMetaData;
    }
}
