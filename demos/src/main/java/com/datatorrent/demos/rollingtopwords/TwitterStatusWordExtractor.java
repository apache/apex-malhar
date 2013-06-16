/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.rollingtopwords;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Arrays;
import java.util.HashSet;
import twitter4j.Status;

/**
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class TwitterStatusWordExtractor extends BaseOperator
{
  public HashSet<String> filterList;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String text)
    {
      String strs[] = text.split(" ");
      if (strs != null) {
        for (String str : strs) {
          if (str != null && !filterList.contains(str) ) {
            output.emit(str);
          }
        }
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    this.filterList = new HashSet<String>(Arrays.asList(new String[]{"", " ","I","you","the","a","to","as","he","him","his","her","she","me","can","for","of","and","or","but",
           "this","that","!",",",".",":","#","/","@","be","in","out","was","were","is","am","are","so","no","...","my","de","RT","on","que","la","i","your","it","have","with","?","when",
    "up","just","do","at","&","-","+","*","\\","y","n","like","se","en","te","el","I'm"}));
  }
}
