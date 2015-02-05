/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface AppDataTopicResultOperator extends AppDataOperator
{
  /**
   * @return the appendQIDToTopic
   */
  public boolean isAppendQIDToTopic();

  /**
   * @param appendQIDToTopic the appendQIDToTopic to set
   */
  public void setAppendQIDToTopic(boolean appendQIDToTopic);
}
