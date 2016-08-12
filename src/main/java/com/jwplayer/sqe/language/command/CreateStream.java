package com.jwplayer.sqe.language.command;

import org.apache.storm.trident.state.StateType;

import java.util.Map;


public class CreateStream extends BaseCommand {
    public String streamName;
    public String objectName;
    public String spoutName;
    public StateType spoutType;
    public String deserializer;
    public Map options;

    @Override
    public CommandType getCommandType() {
        return CommandType.CreateStream;
    }
}
