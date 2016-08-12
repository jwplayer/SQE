package com.jwplayer.sqe.trident.state.redis;

import org.apache.storm.redis.trident.state.KeyFactory;

import java.util.List;

public class RedisKeyFactory implements KeyFactory {
    public String delimiter;
    public List<Integer> keyIndexes;
    public String prefix;
    public String suffix;

    public RedisKeyFactory(String delimiter, List<Integer> keyIndexes, String prefix, String suffix) {
        this.delimiter = delimiter;
        this.keyIndexes = keyIndexes;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    @Override
    public String build(List<Object> keys) {
        StringBuilder sb = new StringBuilder();

        // Append prefix, if it exists
        if(prefix != null && !prefix.equals("")) {
            sb.append(prefix);
            if(keyIndexes.size() > 0 || (suffix != null && !suffix.equals(""))) sb.append(delimiter);
        }

        // Append the keys we want based on their indexes
        for(int i = 0; i < keyIndexes.size() - 1; ++i) {
            sb.append(keys.get(keyIndexes.get(i)));
            sb.append(delimiter);
        }
        if(keyIndexes.size() > 0){
            sb.append(keys.get(keyIndexes.get(keyIndexes.size() - 1)));
        }

        // Append the suffix, if it exists
        if(suffix != null && !suffix.equals("")) {
            if(keyIndexes.size() > 0 || (prefix != null && !prefix.equals(""))) sb.append(delimiter);
            sb.append(suffix);
        }

        return sb.toString();
    }
}
