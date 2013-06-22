/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
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

    public final transient DefaultInputPort<HBaseTuple> inputPort = new DefaultInputPort<HBaseTuple>()
    {
      public void process(HBaseTuple tuple)
      {
        tuples.add(tuple);
      }
    };

}
