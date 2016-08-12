package com.jwplayer.sqe.util;

import kafka.admin.AdminUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.collection.Iterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaTestServer {
    public static final String HOST = "localhost";
    private KafkaServerStartable kafkaServer;
    private File logDir;
    private Integer port;
    KafkaProducer<byte[], byte[]> producer;
    private ZookeeperTestServer zkServer;

    public KafkaTestServer() {
        zkServer = new ZookeeperTestServer();
        port = InstanceSpec.getRandomPort();
        logDir = new File(System.getProperty("java.io.tmpdir"), "kafka/logs/log-" + port.toString());
        logDir.deleteOnExit();
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("broker.id", "0");
        kafkaProperties.setProperty("zookeeper.connect", zkServer.getConnectionString());
        kafkaProperties.setProperty("port", port.toString());
        kafkaProperties.setProperty("log.dirs", logDir.getAbsolutePath());
        kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProperties));
        kafkaServer.startup();
        Properties producerProperties = new Properties();
        producerProperties.setProperty("acks", "all");
        producerProperties.setProperty("bootstrap.servers", getConnectionString());
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProperties);
    }

    public void createTopic(String topic, Integer partitions) {
        AdminUtils.createTopic(zkServer.getZkUtils(), topic, partitions, 1, new Properties());
    }

    public String getConnectionString() {
        return HOST + ":" + port.toString();
    }

    public ZookeeperTestServer getZkServer() {
        return zkServer;
    }

    // This one isn't working for some reason
    public ConsumerRecords<byte[], byte[]> readMessagesBad(String topic) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", getConnectionString());
        consumerProperties.setProperty("group.id", "test");
        consumerProperties.setProperty("enable.auto.commit", "false");
        consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Arrays.asList(topic));
            return consumer.poll(1000);
        }
    }

    public List<MessageAndOffset> readMessages(String topic) {
        SimpleConsumer consumer = new SimpleConsumer(HOST, port, 15000, 1024*1024, "test");
        FetchRequestBuilder builder = new FetchRequestBuilder();
        FetchRequest request = builder.addFetch(topic, 0, 0, 1024*1024).build();
        FetchResponse response = consumer.fetch(request);
        ByteBufferMessageSet messageBuffer = response.messageSet(topic, 0);
        List<MessageAndOffset> messages = new ArrayList<>();
        Iterator<MessageAndOffset> iter = messageBuffer.iterator();

        while (iter.hasNext()) {
            messages.add(iter.next());
        }

        return messages;
    }

    public void sendMessage(String topic, Integer partition, byte[] key, byte[] value)
            throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        producer.flush();
    }

    public void shutdown() {
        kafkaServer.shutdown();
        zkServer.shutdown();
    }
}
