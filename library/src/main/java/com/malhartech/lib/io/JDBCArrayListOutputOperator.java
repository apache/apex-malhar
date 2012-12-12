/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListOutputOperator extends JDBCOutputOperator<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListOutputOperator.class);
  private int count = 0;

  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<ArrayList<AbstractMap.SimpleEntry<String, Object>>> inputPort = new DefaultInputPort<ArrayList<AbstractMap.SimpleEntry<String, Object>>>(this)
  {
    @Override
    public void process(ArrayList<AbstractMap.SimpleEntry<String, Object>> tuple)
    {
      try {
        int num = tuple.size();
        for (int idx=0; idx<num; idx++) {
          String key = tuple.get(idx).getKey();
          getInsertStatement().setObject(
                  getKeyToIndex().get(key).intValue(),
                  tuple.get(idx).getValue(),
                  getColumnSQLTypes().get(getKeyToType().get(key)));
          count++;
        }
        //logger.debug(String.format("ps: %s", getInsertStatement().toString()));
        getInsertStatement().executeUpdate();

      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

      logger.debug(String.format("count %d", count));
    }
  };
}
