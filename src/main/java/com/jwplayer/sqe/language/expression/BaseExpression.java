package com.jwplayer.sqe.language.expression;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseExpression {
    public BaseExpression() { }
    public abstract ExpressionType getExpressionType();
    public abstract String getOutputFieldName();
    public abstract List<BaseExpression> unRoll();

    public static class BaseExpressionTypeAdapter implements JsonDeserializer<BaseExpression>, JsonSerializer<BaseExpression> {
        @Override
        public BaseExpression deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            if(jsonElement.isJsonPrimitive()) {
                JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(primitive.isString()) return new FieldExpression(jsonElement.getAsJsonPrimitive().getAsString());
                else return parseConstant(primitive);
            } else if(jsonElement.isJsonObject()) {
                Set<Map.Entry<String, JsonElement>> entrySet = jsonElement.getAsJsonObject().entrySet();
                if (entrySet.size() != 1) {
                    throw new RuntimeException("The following JSON should contain one and only one element:\n" + jsonElement.getAsString());
                }
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    if (entry.getKey().equals("C")) {
                        if(entry.getValue().isJsonNull()) return new ConstantExpression(null);
                        else return parseConstant(entry.getValue().getAsJsonPrimitive());
                    } else {
                        List<BaseExpression> arguments = deserializeArguments(entry.getValue().getAsJsonArray(),
                                type, jsonDeserializationContext);
                        return FunctionExpression.makeFunction(entry.getKey(), arguments);
                    }
                }
            } else if(jsonElement.isJsonNull()) {
                return new ConstantExpression(null);
            }

            throw new JsonParseException("Could not parse json:\n" + jsonElement.getAsString());
        }

        private List<BaseExpression> deserializeArguments(JsonArray array, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            List<BaseExpression> arguments = new ArrayList<>();
            for(JsonElement jsonElement: array) {
                arguments.add(deserialize(jsonElement, type, jsonDeserializationContext));
            }

            return arguments;
        }

        private ConstantExpression parseConstant(JsonPrimitive primitive) {
            if(primitive.isBoolean()) return new ConstantExpression(primitive.getAsBoolean());
            else if(primitive.isNumber()) {
                // This is to prevent numerical constants from being parsed as BigDecimal, instead opting
                // for something slightly more sane and workable.
                // FIXME: This doesn't work in cultures where . is the 1000s separator
                if(primitive.getAsString().contains("."))
                    return new ConstantExpression(primitive.getAsNumber().doubleValue());
                else {
                    long longValue = primitive.getAsNumber().longValue();
                    if(longValue < Integer.MAX_VALUE)
                        return new ConstantExpression(primitive.getAsNumber().intValue());
                    else
                        return new ConstantExpression(primitive.getAsNumber().longValue());
                }
            }
            else if(primitive.isString()) return new ConstantExpression(primitive.getAsString());
            else throw new JsonParseException("Invalid JSON primitive:\n" + primitive.getAsString());
        }

        @Override
        public JsonElement serialize(BaseExpression expression, Type type, JsonSerializationContext jsonSerializationContext) {
            JsonObject object = new JsonObject();
            switch(expression.getExpressionType()) {
                case Constant:
                    ConstantExpression constant = (ConstantExpression) expression;

                    if(constant.constant == null) return JsonNull.INSTANCE;
                    else if(constant.constant instanceof Boolean) return new JsonPrimitive((Boolean) constant.constant);
                    else if(constant.constant instanceof Number) return new JsonPrimitive((Number) constant.constant);
                    else if(constant.constant instanceof String) object.addProperty("C", (String) constant.constant);
                    else throw new RuntimeException(constant.constant.getClass().getSimpleName() + " is not a valid constant type");

                    return object;
                case Field:
                    return new JsonPrimitive(((FieldExpression) expression).fieldName);
                case Function:
                    FunctionExpression function = (FunctionExpression) expression;
                    JsonArray jsonArray = new JsonArray();
                    for(BaseExpression argument: function.getArguments()) {
                        jsonArray.add(serialize(argument, type, jsonSerializationContext));
                    }
                    object.add(function.getFunctionName(), jsonArray);
                    return object;
                default:
                    throw new RuntimeException(expression.getExpressionType().toString() + " is not a serializable expression type");
            }
        }
    }
}
