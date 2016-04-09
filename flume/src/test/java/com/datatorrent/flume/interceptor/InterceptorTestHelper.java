/*
 * Copyright (c) 2013 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.flume.interceptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.datatorrent.netlet.util.Slice;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class InterceptorTestHelper
{
  private static final byte FIELD_SEPARATOR = 1;

  static class MyEvent implements Event
  {
    byte[] body;

    MyEvent(byte[] bytes)
    {
      body = bytes;
    }

    @Override
    public Map<String, String> getHeaders()
    {
      return null;
    }

    @Override
    public void setHeaders(Map<String, String> map)
    {
    }

    @Override
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public byte[] getBody()
    {
      return body;
    }

    @Override
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public void setBody(byte[] bytes)
    {
      body = bytes;
    }
  }

  private final Interceptor.Builder builder;
  private final Map<String, String> context;

  InterceptorTestHelper(Interceptor.Builder builder, Map<String, String> context)
  {
    this.builder = builder;
    this.context = context;
  }

  public void testIntercept_Event()
  {
    builder.configure(new Context(context));
    Interceptor interceptor = builder.build();

    assertArrayEquals("Empty Bytes",
        "\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("".getBytes())).getBody());

    assertArrayEquals("One Separator",
        "\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("\002".getBytes())).getBody());

    assertArrayEquals("Two Separators",
        "\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("\002\002".getBytes())).getBody());

    assertArrayEquals("One Field",
        "\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First".getBytes())).getBody());

    assertArrayEquals("Two Fields",
        "First\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("\002First".getBytes())).getBody());

    assertArrayEquals("Two Fields",
        "\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\001".getBytes())).getBody());

    assertArrayEquals("Two Fields",
        "Second\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002Second".getBytes())).getBody());

    assertArrayEquals("Three Fields",
        "Second\001\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002Second\002".getBytes())).getBody());

    assertArrayEquals("Three Fields",
        "\001Second\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002\002Second".getBytes())).getBody());

    assertArrayEquals("Four Fields",
        "\001Second\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002\002Second\002".getBytes())).getBody());

    assertArrayEquals("Five Fields",
        "\001Second\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002\002Second\002\002".getBytes())).getBody());

    assertArrayEquals("Six Fields",
        "\001Second\001\001".getBytes(),
        interceptor.intercept(new MyEvent("First\002\002Second\002\002\002".getBytes())).getBody());
  }

  public void testFiles() throws IOException, URISyntaxException
  {
    Properties properties = new Properties();
    properties.load(getClass().getResourceAsStream("/flume/conf/flume-conf.properties"));

    String interceptor = null;
    for (Entry<Object, Object> entry : properties.entrySet()) {
      logger.debug("{} => {}", entry.getKey(), entry.getValue());

      if (builder.getClass().getName().equals(entry.getValue().toString())) {
        String key = entry.getKey().toString();
        if (key.endsWith(".type")) {
          interceptor = key.substring(0, key.length() - "type".length());
          break;
        }
      }
    }

    assertNotNull(builder.getClass().getName(), interceptor);
    @SuppressWarnings({"null", "ConstantConditions"})
    final int interceptorLength = interceptor.length();

    HashMap<String, String> map = new HashMap<String, String>();
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(interceptor)) {
        map.put(key.substring(interceptorLength), entry.getValue().toString());
      }
    }

    builder.configure(new Context(map));
    Interceptor interceptorInstance = builder.build();

    URL url = getClass().getResource("/test_data/gentxns/");
    assertNotNull("Generated Transactions", url);

    int records = 0;
    File dir = new File(url.toURI());
    for (File file : dir.listFiles()) {
      records += processFile(file, interceptorInstance);
    }

    Assert.assertEquals("Total Records", 2200, records);
  }

  private int processFile(File file, Interceptor interceptor) throws IOException
  {
    InputStream stream = getClass().getResourceAsStream("/test_data/gentxns/" + file.getName());
    BufferedReader br = new BufferedReader(new InputStreamReader(stream));

    String line;
    int i = 0;
    while ((line = br.readLine()) != null) {
      byte[] body = interceptor.intercept(new MyEvent(line.getBytes())).getBody();
      RawEvent event = RawEvent.from(body, FIELD_SEPARATOR);
      Assert.assertEquals("GUID", new Slice(line.getBytes(), 0, 32), event.guid);
      logger.debug("guid = {}, time = {}", event.guid, event.time);
      i++;
    }

    br.close();
    return i;
  }

  private static final Logger logger = LoggerFactory.getLogger(InterceptorTestHelper.class);
}
