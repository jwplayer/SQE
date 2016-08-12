package com.jwplayer.sqe.language.command;

import com.google.gson.*;
import com.jwplayer.sqe.language.expression.BaseExpression;

import java.lang.reflect.Type;
import java.util.*;

public abstract class BaseCommand {
    public abstract CommandType getCommandType();

    public static class BaseCommandTypeAdapter implements JsonDeserializer<BaseCommand> {
        protected Gson gson =
                new GsonBuilder()
                .registerTypeAdapter(BaseExpression.class, new BaseExpression.BaseExpressionTypeAdapter())
                .create();

        @Override
        public BaseCommand deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            if(jsonElement.isJsonObject()) {
                Set<Map.Entry<String, JsonElement>> entrySet = jsonElement.getAsJsonObject().entrySet();
                if (entrySet.size() != 1) {
                    throw new RuntimeException("The following JSON should contain one and only one element:\n" + jsonElement.getAsString());
                }
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    return parseCommand(entry.getKey(), entry.getValue());
                }
            }

            throw new JsonParseException("Could not parse json:\n" + jsonElement.getAsString());
        }

        // TODO: Hello factory method, replace?
        public BaseCommand parseCommand(String commandName, JsonElement command) {
            switch(commandName.toLowerCase()) {
                case "createstream":
                    return gson.fromJson(command, CreateStream.class);
                case "query":
                    return gson.fromJson(command, Query.class);
                case "set":
                    return gson.fromJson(command, SetCommand.class);
                default:
                    throw new RuntimeException(commandName + " is not a valid command");
            }
        }
    }

    public static List<BaseCommand> load(String json) {
        GsonBuilder gson = new GsonBuilder();
        gson.registerTypeAdapter(BaseExpression.class, new BaseExpression.BaseExpressionTypeAdapter());
        gson.registerTypeAdapter(BaseCommand.class, new BaseCommand.BaseCommandTypeAdapter());

        BaseCommand[] commands = gson.create().fromJson(json, BaseCommand[].class);

        return Arrays.asList(commands);
    }
}
