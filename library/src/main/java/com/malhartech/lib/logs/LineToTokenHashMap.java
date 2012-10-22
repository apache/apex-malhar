/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all tokens are emitted on output port "tokens"<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, Object><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is "", i.e. tokens are not split<br>
 * <br>
 * Compile time checks<br>
 * None<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * TBD<br>
 *
 * @author amol
 */
public class LineToTokenHashMap extends BaseOperator
{
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String tuple)
    {
      if (tuple.isEmpty()) {
        return;
      }

      String[] tkns = tuple.split(splitby);
      HashMap<String, ArrayList<String>> otuple = new HashMap<String, ArrayList<String>>();
      for (String t: tkns) {
        if (!addToken(t)) {
          continue;
        }
        if (!splittokenby.isEmpty()) {
          String[] vals = t.split(splittokenby);
          if (vals.length == 0) {
            continue;
          }
          String key = vals[0];
          if (!addToken(key)) {
            continue;
          }
          if (vals.length == 1) {
            otuple.put(key, null);
          }
          else {
            ArrayList<String> list = new ArrayList<String>(vals.length);
            for (int i = 1; i < vals.length; i++) {
              list.add(vals[i]);
            }
            otuple.put(vals[0], list);
          }

        }
        else {
          otuple.put(t, null);
        }
      }
      if (!otuple.isEmpty()) {
        tokens.emit(otuple);
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<String, ArrayList<String>>> tokens = new DefaultOutputPort<HashMap<String, ArrayList<String>>>(this);
  String splitby = ";\t ";
  String splittokenby = "";

  public void setSplitby(String str)
  {
    splitby = str;
  }

  public void setSplittokenby(String str)
  {
    splittokenby = str;
  }

  public boolean addToken(String t)
  {
    return !t.isEmpty();
  }
}
