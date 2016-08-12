package com.jwplayer.sqe.util;

import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;


public class YamlConfigTest {
    private Yaml yamlConfig;
    private Map<String, Object> configMap;
    private YamlConfig.InnerConfig multiTypeConfigMap;

    @Before
    public void setUp() throws IOException, URISyntaxException {
        yamlConfig = YamlConfig.getJWConfig();
        configMap = YamlConfig.getJWConfigAsMap();
        multiTypeConfigMap = YamlConfig.getConfig("conf/multi-type-props.yaml");
    }

    @Test
    public void testConfigMap() {
        assertTrue(configMap.containsKey("SQE"));
    }

    @Test
    public void testBoolean() {
        assertTrue("foo", multiTypeConfigMap.getBoolean("bool", false));
    }

    @Test
    public void testMap(){
        assertNotNull("map", multiTypeConfigMap.getMap("map", null));
    }

    @Test
    public void testInnerConfig() throws IOException {
        YamlConfig.InnerConfig config = multiTypeConfigMap.getInnerConfig("map", null);
        assertNotNull(config);
        assertEquals(config.getInteger("hp", 0), 13);
        assertEquals(config.getString("name", ""), "SusyTheBarbarian");
    }

    @Test
    public void testInteger(){
        assertEquals(multiTypeConfigMap.getInteger("int", 0), 42);
    }

    @Test
    public void testDouble(){
        assertEquals(multiTypeConfigMap.getDouble("float", 66.6), 3.14159);
    }

    @Test
    public void testList(){
        assertEquals(multiTypeConfigMap.getList("list", null).size(), 3);
    }
}
