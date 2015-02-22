/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import java.text.ParseException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DateTest
{
  private static final Logger logger = LoggerFactory.getLogger(DateTest.class);

  @Test
  public void dateTest() throws ParseException
  {
    String dateString = AdsTimeRangeBucket.sdf.format(new Date(1424479164495L));
    logger.debug("{}", dateString);

    logger.debug("{}, {}", 1424479164495L, AdsTimeRangeBucket.sdf.parse(dateString).getTime());
    //1424469420000
  }
}
