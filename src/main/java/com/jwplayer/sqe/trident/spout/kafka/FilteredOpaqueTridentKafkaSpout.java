package com.jwplayer.sqe.trident.spout.kafka;

import org.apache.storm.kafka.Partition;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;


public class FilteredOpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<List<GlobalPartitionInformation>, Partition, Map> {
    private TridentKafkaConfig config;
    private long hwmTtl;
    private OpaqueTridentKafkaSpout spout;

    public FilteredOpaqueTridentKafkaSpout(TridentKafkaConfig config, long hwmTtl) {
        this.config = config;
        this.hwmTtl = hwmTtl;
        this.spout = new OpaqueTridentKafkaSpout(config);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
        return spout.getCoordinator(map, topologyContext);
    }

    @Override
    public Emitter<List<GlobalPartitionInformation>, Partition, Map> getEmitter(Map conf, TopologyContext context) {
        return (new FilteredTridentKafkaEmitter(conf, context, config, context.getStormId(), hwmTtl)).asOpaqueEmitter();
    }

    @Override
    public Fields getOutputFields() {
        return spout.getOutputFields();
    }
}
