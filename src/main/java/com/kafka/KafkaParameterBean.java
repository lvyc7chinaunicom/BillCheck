package com.kafka;

import java.util.List;

/**
 * kafkaUtli类中需要的参数bean
 *
 * @author lvyc7
 * @date 2018-11-28 15:13
 */
public class KafkaParameterBean {
    private long a_maxReads;
    private String a_topic;
    private int a_partition;
    private List<String> a_seedBrokers;
    private List<Integer> a_ports;

    public KafkaParameterBean(long a_maxReads, String a_topic,
                              int a_partition, List<String> a_seedBrokers, List<Integer> a_ports) {
        this.a_maxReads = a_maxReads;
        this.a_topic = a_topic;
        this.a_partition = a_partition;
        this.a_seedBrokers = a_seedBrokers;
        this.a_ports = a_ports;
    }

    public long getA_maxReads() {
        return a_maxReads;
    }

    public void setA_maxReads(long a_maxReads) {
        this.a_maxReads = a_maxReads;
    }

    public String getA_topic() {
        return a_topic;
    }

    public void setA_topic(String a_topic) {
        this.a_topic = a_topic;
    }

    public int getA_partition() {
        return a_partition;
    }

    public void setA_partition(int a_partition) {
        this.a_partition = a_partition;
    }

    public List<String> getA_seedBrokers() {
        return a_seedBrokers;
    }

    public void setA_seedBrokers(List<String> a_seedBrokers) {
        this.a_seedBrokers = a_seedBrokers;
    }

    public List<Integer> getA_ports() {
        return a_ports;
    }

    public void setA_ports(List<Integer> a_ports) {
        this.a_ports = a_ports;
    }
}
