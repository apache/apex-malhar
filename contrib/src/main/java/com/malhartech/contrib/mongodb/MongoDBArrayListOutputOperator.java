/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBArrayListOutputOperator extends MongoDBOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBArrayListOutputOperator.class);

  @Override
  public void processTuple(ArrayList<Object> tuple)
  {
    
  }

}
