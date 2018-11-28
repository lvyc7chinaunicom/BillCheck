package com.kafka;

import com.sun.org.apache.xerces.internal.dom.PSVIAttrNSImpl;
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
 * test
 *
 * @author lvyc7
 * @date 2018-11-28 16:17
 */
public class test {

    private static final Logger logger = Logger.getLogger(test.class);
    private List<String> m_replicaBrokers = new ArrayList<>();
    private List<Integer> m_replicaPorts = new ArrayList<>();
    //redis中记录的已读偏移
    private long targetOffset = -1;
    //消费到的msg列表
    private ArrayList<String> msgList = new ArrayList<>();
    //一次消费的量
    private long maxReads = 10;

    List<String> brokers = Arrays.asList("10.161.11.156", "10.161.11.157", "10.161.11.158", "10.161.11.159", "10.161.11.160", "10.161.11.161", "10.161.11.162", "10.161.11.163", "10.161.11.164", "10.161.11.165", "10.161.11.166", "10.161.11.167", "10.161.11.168", "10.161.11.169", "10.161.11.170");
    List<Integer> ports = new ArrayList<>(Arrays.asList(6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667, 6667));
    String topic = "CdrNormal";
    int partition = 0;

    public static void main(String[] args) {
        new test().tt();

    }

    public void tt() {
        try {
            for (int j = 0; j < 6; j++) {
                getMsg(msgList, maxReads, targetOffset, topic, partition, brokers, ports);
                if (msgList.size() != 0) {
                    msgList.stream().forEach(System.out::println);
                } else {
                    System.out.println("没有取到数据");
                }
                targetOffset += maxReads;
                System.out.println("targetOffset:" + targetOffset);
                msgList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getMsg(ArrayList<String> msgList, long a_maxReads, long targetOffset, String a_topic,
                        int a_partition, List<String> a_seedBrokers, List<Integer> a_ports) throws Exception {
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_ports, a_topic, a_partition);
        if (metadata == null) {
            logger.error("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            logger.error("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        int a_port = metadata.leader().port();
        String clientName = "Client_" + a_topic + "_" + a_partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 10000, 64 * 1024, clientName);
        // kafka.api.OffsetRequest.EarliestTime() finds the beginning of the data in the logs and starts streaming from there
        long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        long latestOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);

        System.out.println("***************");
        System.out.println(readOffset);
        System.out.println(latestOffset);
        System.out.println("***************");

        //若目标偏移大于kafka中最新偏移
        if (targetOffset > latestOffset) {
            //将要读的偏移置为最新
            readOffset = latestOffset;
        } else {
            if (targetOffset > readOffset) {
                //目则将要读的偏移置为目标偏移
                readOffset = targetOffset;
            }
        }
        this.targetOffset = readOffset;
        //System.out.println("offset:" + readOffset);
        int numErrors = 0;
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 10000, 64 * 1024, clientName);
            }
            // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(a_topic, a_partition, readOffset, 64 * 1024)
                    .build();

            FetchResponse fetchResponse = consumer.fetch(req);

            //Identify and recover from leader changes
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                StringBuilder errStr = new StringBuilder();
                logger.error(errStr.append("Error fetching data from the Broker:").append(" Reason: ").append(String.valueOf(code)).toString());
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                //查找新的leader
                metadata = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                leadBroker = metadata.leader().host();
                a_port = metadata.leader().port();
                continue;
            }
            numErrors = 0;

            //Fetch the data
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                if (a_maxReads > 0) {
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
                    msgList.add(new String(bytes, "gb2312"));
                    numRead++;
                    a_maxReads--;
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
        if (consumer != null) consumer.close();
        return;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
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
