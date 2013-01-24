/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.rollingtopwords;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import twitter4j.Status;

/**
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class TwitterStatusWordExtractor extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
  public final transient DefaultInputPort<Status> input = new DefaultInputPort<Status>(this)
  {
    @Override
    public void process(Status status)
    {
      String strs[] = status.getText().split(" ");
      if (strs != null) {
        for (String str : strs) {
          if (str != null && !str.equals(" ") && !str.equals("")) {
            output.emit(str);
          }
        }
      }
    }
  };
}
