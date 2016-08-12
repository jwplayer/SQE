package com.jwplayer.sqe.language.serde.avro;

import java.util.Map;

public class AvroDeserializerOptions {
    public String schemaName = null;

    public static AvroDeserializerOptions parse(Map map) {
        AvroDeserializerOptions options = new AvroDeserializerOptions();

        if(map.containsKey("jw.sqe.spout.deserializer.avro.schemaname"))
            options.schemaName = (String) map.get("jw.sqe.spout.deserializer.avro.schemaname");

        return options;
    }
}
