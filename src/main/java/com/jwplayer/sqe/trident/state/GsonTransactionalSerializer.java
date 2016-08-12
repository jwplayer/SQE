package com.jwplayer.sqe.trident.state;

import com.google.gson.*;
import org.apache.storm.trident.state.TransactionalValue;

import java.io.UnsupportedEncodingException;


public class GsonTransactionalSerializer extends BaseGsonSerializer<TransactionalValue> {
    public byte[] serialize(TransactionalValue tValue) {
        try {
            Gson gson = new Gson();
            JsonArray jsonArray = new JsonArray();
            JsonPrimitive txID = new JsonPrimitive(tValue.getTxid());
            jsonArray.add(txID);
            jsonArray.addAll(serializeType(tValue.getVal()));

            return gson.toJson(jsonArray).getBytes("UTF-8");
        } catch(UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransactionalValue deserialize(byte[] bytes) {
        try {
            Gson gson = new Gson();
            String json = new String(bytes, "UTF-8");
            JsonArray jsonArray = gson.fromJson(json, JsonArray.class);
            Long txID = jsonArray.get(0).getAsLong();

            return new TransactionalValue(txID, deserializeType(jsonArray, 1, 2));
        } catch(UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
