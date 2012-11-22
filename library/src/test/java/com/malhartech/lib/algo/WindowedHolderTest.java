/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.esotericsoftware.kryo.Kryo;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.engine.DefaultStreamCodec;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowedHolderTest
{
  public WindowedHolderTest()
  {
  }

  @Test
  public void testKryo()
  {
    WindowedHolder<String> windowedHolder1 = new WindowedHolder<String>("one", 2);
    windowedHolder1.adjustCount(1);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 1);
    windowedHolder1.adjustCount(2);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 2);
    windowedHolder1.adjustCount(3);

    assertEquals("count at exiting position", windowedHolder1.windowedCount[0], 3);
    assertEquals("count at new position", windowedHolder1.windowedCount[1], 2);


    DefaultStreamCodec dsc = new DefaultStreamCodec();
    DataStatePair dsp = dsc.toByteArray(windowedHolder1);
    @SuppressWarnings("unchecked")
    WindowedHolder<String> windowedHolder2 = (WindowedHolder<String>)dsc.fromByteArray(dsp);

    assertEquals("count at exiting position", windowedHolder2.windowedCount[0], 3);
    assertEquals("count at new position", windowedHolder2.windowedCount[1], 2);
    assertEquals("total count", windowedHolder2.totalCount, 2);
  }
}
