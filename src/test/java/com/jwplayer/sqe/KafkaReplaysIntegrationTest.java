package com.jwplayer.sqe;

import com.jwplayer.sqe.trident.StreamMetadata;
import com.jwplayer.sqe.util.KafkaTestServer;
import kafka.message.MessageAndOffset;
import org.apache.commons.io.IOUtils;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;


public class KafkaReplaysIntegrationTest {
    private final String SOURCE_TOPIC = "source";
    private final String DEST_TOPIC = "dest";
    private KafkaTestServer kafkaServer;
    private List<MessageAndOffset> destMessages;
    private List<MessageAndOffset> sourceMessages;

    @Before
    public void setup() throws Exception {
        kafkaServer = new KafkaTestServer();

        // Populate the source topic

        // Partition 0
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 0).toBytes(), new StreamMetadata(0L, 0, 0).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 1).toBytes(), new StreamMetadata(0L, 0, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 2).toBytes(), new StreamMetadata(0L, 0, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 1).toBytes(), new StreamMetadata(0L, 0, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 2).toBytes(), new StreamMetadata(0L, 0, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 3).toBytes(), new StreamMetadata(0L, 0, 3).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 2).toBytes(), new StreamMetadata(0L, 0, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 3).toBytes(), new StreamMetadata(0L, 0, 3).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 0, 4).toBytes(), new StreamMetadata(0L, 0, 4).toBytes());

        // Partition 1
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 0).toBytes(), new StreamMetadata(0L, 1, 0).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 1).toBytes(), new StreamMetadata(0L, 1, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 2).toBytes(), new StreamMetadata(0L, 1, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 1).toBytes(), new StreamMetadata(0L, 1, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 0).toBytes(), new StreamMetadata(0L, 1, 0).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 1).toBytes(), new StreamMetadata(0L, 1, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 2).toBytes(), new StreamMetadata(0L, 1, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 1).toBytes(), new StreamMetadata(0L, 1, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 1, 0).toBytes(), new StreamMetadata(0L, 1, 0).toBytes());

        // Partition 2
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 0).toBytes(), new StreamMetadata(0L, 2, 0).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 1).toBytes(), new StreamMetadata(0L, 2, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 2).toBytes(), new StreamMetadata(0L, 2, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 3).toBytes(), new StreamMetadata(0L, 2, 3).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 4).toBytes(), new StreamMetadata(0L, 2, 4).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 3).toBytes(), new StreamMetadata(0L, 2, 3).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 2).toBytes(), new StreamMetadata(0L, 2, 2).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 1).toBytes(), new StreamMetadata(0L, 2, 1).toBytes());
        kafkaServer.sendMessage(SOURCE_TOPIC, 0, new StreamMetadata(0L, 2, 0).toBytes(), new StreamMetadata(0L, 2, 0).toBytes());

        sourceMessages = kafkaServer.readMessages(SOURCE_TOPIC);

        // Create command file
        String commands = IOUtils.toString(this.getClass().getResourceAsStream("/queries/kafka-replays.json"));
        commands = String.format(commands, kafkaServer.getZkServer().getConnectionString(), kafkaServer.getConnectionString());
        File commandsFile = File.createTempFile("kafka-replays", "json");
        commandsFile.deleteOnExit();
        PrintWriter writer = new PrintWriter(commandsFile);
        writer.println(commands);
        writer.close();

        // Run test topologies
        String[] args = new String[9];

        args[0] = "--commands";
        args[1] = commandsFile.toURI().toString();
        args[2] = "--config";
        args[3] = "./conf/sample-conf.yaml";
        args[4] = "--local";
        args[5] = "--name";
        args[6] = "KafkaReplays";
        args[7] = "--runtime";
        args[8] = "120";

        Topology.main(args);
        destMessages = kafkaServer.readMessages(DEST_TOPIC);
        kafkaServer.shutdown();
    }

    @Test
    public void destMessagesTest() {
        // We expect 13 of 27 messages to make it to the destination topic
        Assert.assertEquals(destMessages.size(), 13);

        // We don't know the pid, but we can pull it from the first message. It should be consistent
        // across all messages. We *do* know the expected ordering of the offsets from the source topic
        // and the partition.
        StreamMetadata metadata = StreamMetadata.parseBytes(Utils.toByteArray(destMessages.get(0).message().key()));
        long pid = metadata.pid;

        // Test each message
        testMessage(destMessages.get(0), new StreamMetadata(pid, 0, 0).toBytes(), new StreamMetadata(0L, 0, 0).toBytes());
        testMessage(destMessages.get(1), new StreamMetadata(pid, 0, 1).toBytes(), new StreamMetadata(0L, 0, 1).toBytes());
        testMessage(destMessages.get(2), new StreamMetadata(pid, 0, 2).toBytes(), new StreamMetadata(0L, 0, 2).toBytes());
        testMessage(destMessages.get(3), new StreamMetadata(pid, 0, 5).toBytes(), new StreamMetadata(0L, 0, 3).toBytes());
        testMessage(destMessages.get(4), new StreamMetadata(pid, 0, 8).toBytes(), new StreamMetadata(0L, 0, 4).toBytes());
        testMessage(destMessages.get(5), new StreamMetadata(pid, 0, 9).toBytes(), new StreamMetadata(0L, 1, 0).toBytes());
        testMessage(destMessages.get(6), new StreamMetadata(pid, 0, 10).toBytes(), new StreamMetadata(0L, 1, 1).toBytes());
        testMessage(destMessages.get(7), new StreamMetadata(pid, 0, 11).toBytes(), new StreamMetadata(0L, 1, 2).toBytes());
        testMessage(destMessages.get(8), new StreamMetadata(pid, 0, 18).toBytes(), new StreamMetadata(0L, 2, 0).toBytes());
        testMessage(destMessages.get(9), new StreamMetadata(pid, 0, 19).toBytes(), new StreamMetadata(0L, 2, 1).toBytes());
        testMessage(destMessages.get(10), new StreamMetadata(pid, 0, 20).toBytes(), new StreamMetadata(0L, 2, 2).toBytes());
        testMessage(destMessages.get(11), new StreamMetadata(pid, 0, 21).toBytes(), new StreamMetadata(0L, 2, 3).toBytes());
        testMessage(destMessages.get(12), new StreamMetadata(pid, 0, 22).toBytes(), new StreamMetadata(0L, 2, 4).toBytes());
    }

    @Test
    public void soureMessagesTest() {
        Assert.assertEquals(sourceMessages.size(), 27);
    }

    public void testMessage(MessageAndOffset messageAndOffset, byte[] expectedKey, byte[] expectedValue) {
        byte[] key = Utils.toByteArray(messageAndOffset.message().key());
        byte[] value = Utils.toByteArray(messageAndOffset.message().payload());

        Assert.assertArrayEquals(expectedKey, key);
        Assert.assertArrayEquals(expectedValue, value);
    }
}
