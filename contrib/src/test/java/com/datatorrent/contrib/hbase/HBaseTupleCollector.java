/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseTupleCollector extends BaseOperator
{

    public static List<HBaseTuple> tuples;

    public HBaseTupleCollector()
    {
      tuples = new ArrayList<HBaseTuple>();
    }

    public final transient DefaultInputPort<HBaseTuple> inputPort = new DefaultInputPort<HBaseTuple>(this)
    {
      public void process(HBaseTuple tuple)
      {
        tuples.add(tuple);
      }
    };

}
