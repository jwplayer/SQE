package com.jwplayer.sqe.language.serde;


import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;

public class IdentityDeserializer extends BaseDeserializer {
    public void IdentityDeserializer() {

    }

    @Override
    public Stream deserialize(Stream stream, Fields requiredFields) {
        return stream;
    }
}
