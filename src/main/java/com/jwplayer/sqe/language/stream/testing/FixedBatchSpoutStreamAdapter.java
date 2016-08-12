package com.jwplayer.sqe.language.stream.testing;

import com.jwplayer.sqe.language.stream.StreamAdapter;
import com.jwplayer.sqe.trident.StreamMetadata;
import com.jwplayer.sqe.trident.function.AddField;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;


public class FixedBatchSpoutStreamAdapter extends StreamAdapter {
    FixedBatchSpoutOptions options;

    public FixedBatchSpoutStreamAdapter(FixedBatchSpoutOptions options) {
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream makeStream(TridentTopology topology, String topologyName, String streamName, String objectName, StateType spoutType) {
        List<Object>[] values = (List<Object>[]) options.values.toArray(new List[options.values.size()]);
        FixedBatchSpout spout = new FixedBatchSpout(options.fields, options.values.size(), values);
        Stream stream = topology.newStream(topologyName + "/" + streamName, spout);
        long pid = StreamAdapter.createPid(topologyName, streamName, "");
        StreamMetadata metadata = new StreamMetadata(pid, 0, 0L);
        stream = stream.each(new Fields(), new AddField(metadata), new Fields(StreamAdapter.STREAM_METADATA_FIELD));
        return stream;
    }
}
