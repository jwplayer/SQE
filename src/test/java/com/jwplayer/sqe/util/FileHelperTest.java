package com.jwplayer.sqe.util;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNotSame;


public class FileHelperTest {
    @Test
    public void testRelativeURI() throws IOException, URISyntaxException {
        URI uri = new URI("conf/sample-conf.yaml");
        String text = FileHelper.loadFileAsString(uri);

        assertNotNull(text);
        assertNotSame(text, "");
    }

    @Test
    public void testFileURI() throws IOException, URISyntaxException {
        URI currentPath = Paths.get("").toAbsolutePath().toUri();
        URI uri = new URI(currentPath + "conf/sample-conf.yaml");
        String text = FileHelper.loadFileAsString(uri);

        assertNotNull(text);
        assertNotSame(text, "");
    }

    @Test
    public void testResourceURI() throws IOException, URISyntaxException {
        URI uri = new URI("resource:///sample-resource.txt");
        String text = FileHelper.loadFileAsString(uri);

        assertNotNull(text);
        assertNotSame(text, "");
    }
}
