package com.jwplayer.sqe.language.serde;

import com.jwplayer.sqe.language.serde.avro.AvroDeserializer;
import com.jwplayer.sqe.language.serde.avro.AvroDeserializerOptions;
import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;

import java.util.Map;


public abstract class BaseDeserializer {
    public abstract Stream deserialize(Stream stream, Fields requiredFields);

    public static BaseDeserializer makeDeserializer(String deserializerName, Map deserializerOptions) {
        switch(deserializerName.toLowerCase()) {
            case "avro":
                AvroDeserializerOptions avroOptions = AvroDeserializerOptions.parse(deserializerOptions);
                return new AvroDeserializer(avroOptions);
            case "identity":
                return new IdentityDeserializer();
            default:
                throw new RuntimeException(deserializerName + " is not a supported deserializer");
        }
    }
}
