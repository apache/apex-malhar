/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.twitter;

import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import com.datatorrent.lib.appdata.schemas.twitter.TwitterDataValues;
import com.datatorrent.lib.appdata.schemas.twitter.TwitterOneTimeResult.TwitterData;
import com.datatorrent.lib.appdata.schemas.twitter.TwitterUpdateQuery;
import com.datatorrent.lib.appdata.schemas.twitter.TwitterUpdateResult;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TwitterUpdateResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterUpdateResultTest.class);

  @Test
  public void testSerialization()
  {
    ResultSerializerFactory rsf = new ResultSerializerFactory();

    TwitterUpdateQuery tuq = new TwitterUpdateQuery();
    tuq.setId("1");
    tuq.setType("updateQuery");

    TwitterUpdateResult tur = new TwitterUpdateResult(tuq);
    tur.setCountdown(1);

    TwitterData twd = new TwitterData();

    List<TwitterDataValues> twitterValues = Lists.newArrayList();

    TwitterDataValues tdv1 = new TwitterDataValues();
    tdv1.setUrl("http://www.google.com");
    tdv1.setCount(1);

    twitterValues.add(tdv1);

    twd.setValues(twitterValues);
    tur.setData(twd);

    String turJSON = rsf.serialize(tur);
    logger.debug(turJSON);
  }
}
