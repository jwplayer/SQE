package com.jwplayer.sqe.language.command;

public class SetCommand extends BaseCommand {
    public String key;
    public Object value;

    @Override
    public CommandType getCommandType() {
        return CommandType.Set;
    }
}
