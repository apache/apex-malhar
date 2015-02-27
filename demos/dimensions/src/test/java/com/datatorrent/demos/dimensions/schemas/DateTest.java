/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.demos.dimensions.schemas.AdsTimeRangeBucket;
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
    String dateString = AdsTimeRangeBucket.sdf.format(new Date(1424637180000L));
    logger.debug("{}", dateString);

    logger.debug("{}, {}",
                 AdsTimeRangeBucket.sdf.format(AdsTimeRangeBucket.sdf.parse(dateString).getTime()),
                 AdsTimeRangeBucket.sdf.parse(dateString).getTime());
    //1424469420000
  }
}
