/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataQueryTabularTest
{
  @Test
  public void simpleDataQueryTabularOneTime()
  {
    Fields fields = new Fields(Sets.newHashSet("a", "b"));

    GenericDataQueryTabular query = new GenericDataQueryTabular("1",
                                                               "dataQuery",
                                                                fields);

    Assert.assertEquals("This query should be oneTime.", true, query.isOneTime());
  }

  @Test
  public void simpeDataQueryTabularCountdown()
  {
    Fields fields = new Fields(Sets.newHashSet("a", "b"));

    GenericDataQueryTabular query = new GenericDataQueryTabular("1",
                                                               "dataQuery",
                                                                fields,
                                                                1L);

    Assert.assertEquals("This query should be oneTime.", false, query.isOneTime());
  }
}
