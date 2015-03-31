/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataSerializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@DataType(type=TwitterUpdateResult.TYPE)
@DataSerializerInfo(clazz=SimpleDataSerializer.class)
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
