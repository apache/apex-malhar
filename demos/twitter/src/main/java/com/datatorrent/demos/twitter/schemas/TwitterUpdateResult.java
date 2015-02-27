/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=TwitterUpdateResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class TwitterUpdateResult extends TwitterOneTimeResult
{
  public static final String TYPE = "updateData";

  private Long countdown;

  public TwitterUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the countdown
   */
  public Long getCountdown()
  {
    return countdown;
  }

  /**
   * @param countdown the countdown to set
   */
  public void setCountdown(Long countdown)
  {
    this.countdown = countdown;
  }
}
