/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.demos.twitter.URLHolderSerde;
import com.malhartech.demos.twitter.WindowedHolder;
import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.junit.Ignore;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Ignore
public class URLHolderSerdeTest extends TestCase
{
  public URLHolderSerdeTest(String testName)
  {
    super(testName);
  }

  public void test()
  {
    WindowedHolder wuh = new WindowedHolder(ByteBuffer.wrap("http://chetan.narsude.net".
      getBytes()), 10);
    wuh.totalCount = 20;
    URLHolderSerde instance = new URLHolderSerde();

    byte[] serialized = instance.toByteArray(wuh);

    WindowedHolder wuh1 = (WindowedHolder) instance.fromByteArray(serialized);

//    assertEquals(wuh.url, wuh1.url);
    assertEquals(wuh.totalCount, wuh1.totalCount);

    wuh = new WindowedHolder(ByteBuffer.wrap("http://www.narsude.com".getBytes()), 10);
    wuh.totalCount = 30;

    serialized = instance.toByteArray(wuh);

    wuh1 = (WindowedHolder) instance.fromByteArray(serialized);

//    assertEquals(wuh.url, wuh1.url);
    assertEquals(wuh.totalCount, wuh1.totalCount);

  }

}
