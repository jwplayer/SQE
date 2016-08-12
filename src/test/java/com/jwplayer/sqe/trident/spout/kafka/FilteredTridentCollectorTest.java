package com.jwplayer.sqe.trident.spout.kafka;

import static org.junit.Assert.*;

import com.jwplayer.sqe.trident.ListValuesCollector;
import com.jwplayer.sqe.trident.StreamMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FilteredTridentCollectorTest {
    public static final long hwmTtl = 1L;

    @Test
    public void testFilter() throws InterruptedException {
        StreamMetadata streamMetadata1 = new StreamMetadata(0, 0, 1);
        List<Object> values1 = new ArrayList<>();
        values1.add(streamMetadata1.toBytes());
        StreamMetadata streamMetadata2 = new StreamMetadata(0, 0, 2);
        List<Object> values2 = new ArrayList<>();
        values2.add(streamMetadata2.toBytes());
        StreamMetadata streamMetadata3 = new StreamMetadata(0, 0, 3);
        List<Object> values3 = new ArrayList<>();
        values3.add(streamMetadata3.toBytes());
        ListValuesCollector collector = new ListValuesCollector();
        FilteredTridentCollector filteredCollector = new FilteredTridentCollector(collector, 0, hwmTtl, new HashMap());

        filteredCollector.emit(values1);
        filteredCollector.emit(values2);
        filteredCollector.emit(values1);
        filteredCollector.emit(values2);
        filteredCollector.emit(values3);
        filteredCollector.emit(values2);
        filteredCollector.emit(values3);

        assertEquals(collector.values.size(), 3);
        assertEquals(collector.values.get(0), values1);
        assertEquals(collector.values.get(1), values2);
        assertEquals(collector.values.get(2), values3);

        Map newMetadata = new HashMap();
        filteredCollector.resolveMetadata(newMetadata);

        assertTrue(newMetadata.containsKey(FilteredTridentCollector.HIGH_WATERMARKS));
        Map<String, Map<String, Long>> hwmMetadata = (Map) newMetadata.get(FilteredTridentCollector.HIGH_WATERMARKS);
        assertEquals(hwmMetadata.size(), 1);
        assertEquals(hwmMetadata.get("0-0").get(FilteredTridentCollector.WATERMARK), (Long) 3L);

        TimeUnit.SECONDS.sleep(2L);
        filteredCollector.resolveMetadata(newMetadata);
        assertEquals(hwmMetadata.size(), 0);
    }
}
