package com.jwplayer.sqe.language.stream;

import org.apache.storm.trident.Stream;

public class StreamContainer {
    public InputStreamType inputStreamType;
    public Stream stream;
    public String streamName;

    public StreamContainer(InputStreamType inputStreamType, Stream stream, String streamName) {
        this.inputStreamType = inputStreamType;
        this.stream = stream;
        this.streamName = streamName;
    }
}
