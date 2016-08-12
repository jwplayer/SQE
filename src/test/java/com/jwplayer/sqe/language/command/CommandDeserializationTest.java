package com.jwplayer.sqe.language.command;

import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class CommandDeserializationTest {
    List<BaseCommand> commands;

    @Before
    public void setup() throws IOException {
        String json = IOUtils.toString(this.getClass().getResourceAsStream("/queries/command-deserialization.json"));
        commands = BaseCommand.load(json);
    }

    @Test
    public void testLoadCommand() {
        assertEquals(commands.size(), 4);
    }

    @Test
    public void testSetCommand() {
        assertEquals(commands.get(0).getCommandType(), CommandType.Set);

        SetCommand setCommand = (SetCommand) commands.get(0);

        assertEquals(setCommand.key, "jw.sqe.state.redis.host");
        assertEquals(setCommand.value, "localhost");
    }

    @Test
    public void testQueryCommand() {
        assertEquals(commands.get(2).getCommandType(), CommandType.Query);

        Query query = (Query) commands.get(2);

        assertNotNull(query.insertInto);
        assertEquals(query.insertInto.objectName, "TestComplexQuery");
        assertNotNull(query.select);
        assertEquals(query.select.expressions.size(), 4);
        assertNotNull(query.from.objectName, "big.query.data");
        assertNotNull(query.where);
    }
}
