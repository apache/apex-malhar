/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.multiwindow;

/**
 * Interface to store window specific information.
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public interface SlidingWindowObject
{
  /**
   * Clear out the fields as needed when streaming window moves to next one. This method is called at the beginning of each window.
   */
  public void clear();

}
