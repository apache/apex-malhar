/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCHashMapOutputOperator<V> extends JDBCOutputOperator<V>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCHashMapOutputOperator.class);
  private int count = 0;

    /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<HashMap<String, V>> inputPort = new DefaultInputPort<HashMap<String, V>>(this)
  {
    @Override
    public void process(HashMap<String, V> tuple)
    {
      try {
        for (Map.Entry<String, V> e: tuple.entrySet()) {
          getInsertStatement().setString(getKeyToIndex().get(e.getKey()).intValue(), e.getValue().toString());
          count++;
        }
        getInsertStatement().executeUpdate();
      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

      logger.debug(String.format("count %d", count));
    }
  };
}
