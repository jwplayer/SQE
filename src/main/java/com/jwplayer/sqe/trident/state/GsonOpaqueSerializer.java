package com.jwplayer.sqe.trident.state;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.apache.storm.trident.state.OpaqueValue;

import java.io.UnsupportedEncodingException;


public class GsonOpaqueSerializer extends BaseGsonSerializer<OpaqueValue> {
    public byte[] serialize(OpaqueValue value) {
        try {
            Gson gson = new Gson();
            JsonArray jsonArray = new JsonArray();
            JsonPrimitive txID = new JsonPrimitive(value.getCurrTxid());
            jsonArray.add(txID);
            jsonArray.addAll(serializeType(value.getCurr()));
            jsonArray.addAll(serializeType(value.getPrev()));

            return gson.toJson(jsonArray).getBytes("UTF-8");
        } catch(UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public OpaqueValue deserialize(byte[] bytes) {
        try {
            Gson gson = new Gson();
            String json = new String(bytes, "UTF-8");
            JsonArray jsonArray = gson.fromJson(json, JsonArray.class);
            Long txID = jsonArray.get(0).getAsLong();

            if(jsonArray.size() == 3) {
                return new OpaqueValue(txID, deserializeType(jsonArray, 1, 2));
            }
            else {
                return new OpaqueValue(txID, deserializeType(jsonArray, 1, 2), deserializeType(jsonArray, 3, 4));
            }
        } catch(UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
