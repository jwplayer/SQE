package com.jwplayer.sqe.language.serde.avro;

import com.jwplayer.sqe.language.serde.BaseDeserializer;
import com.jwplayer.sqe.trident.function.GetTupleFromAvro;
import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;


public class AvroDeserializer extends BaseDeserializer {
    AvroDeserializerOptions options;

    public AvroDeserializer(AvroDeserializerOptions options) {
        this.options = options;
    }

    @Override
    public Stream deserialize(Stream stream, Fields requiredFields) {
        return stream
                .each(stream.getOutputFields(), new GetTupleFromAvro(options.schemaName, requiredFields), requiredFields);
    }
}
