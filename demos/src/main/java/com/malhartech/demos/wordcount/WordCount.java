/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class WordCount extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(WordCount.class);
  public static Map<String, Integer> counts = new HashMap<String, Integer>();
  public static int words = 0;

  public transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String word)
    {
            Integer count = counts.get(word);
            if(count==null) count = 0;
            count++;
            counts.put(word, count);
            ArrayList<Object> al = new ArrayList<Object>();
            al.add(word);
            al.add(count);
            output.emit(al);
            ++words;
    }
  };

  public transient DefaultOutputPort<ArrayList<Object>> output = new DefaultOutputPort<ArrayList<Object>>(this);

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
//    logger.debug("WordCount END words:"+words+" size:"+counts.size()+","+counts.toString());
  }

  @Override
  public void teardown()
  {
    ArrayList sortedKeys = new ArrayList(counts.keySet());
    Collections.sort(sortedKeys);
  }
}
