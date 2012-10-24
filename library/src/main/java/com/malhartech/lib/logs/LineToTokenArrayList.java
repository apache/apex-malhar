/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;


import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tkns. An ArrayList of all tkns are emitted on output port "tkns"<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tkns</b>: Output port, emits ArrayList<Object><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is "", i.e. tkns are not split<br>
 * <b>filterby</b>: The keys to be filters. If a key is not in this comma separated list it is ignored<br>
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
public class LineToTokenArrayList extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String tuple)
    {
      if (tuple.isEmpty()) {
        // emit error token?
        return;
      }
      String[] tkns = tuple.split(splitby);
      ArrayList<String> tokentuple = null;
      ArrayList<HashMap<String, ArrayList<String>>> stokentuple = null;
      for (String t: tkns) {
        if (!addToken(t)) {
          continue;
        }
        if (tokens.isConnected()) {
          if (tokentuple == null) {
            tokentuple = new ArrayList<String>(tkns.length);
          }
          tokentuple.add(t);
        }
        if (splittokens.isConnected() && (splittokenby != null) && !splittokenby.isEmpty()) {
          if (stokentuple == null) {
            stokentuple = new ArrayList<HashMap<String, ArrayList<String>>>(tkns.length);
          }
          String[] vals = t.split(splittokenby);
          if (vals.length != 0) {
            String key = vals[0];
            if (!addToken(key)) {
              continue;
            }
            HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>(4);
            if (vals.length == 1) {
              map.put(key, null);
              stokentuple.add(map);
            }
            else if (vals.length == 2) {
              ArrayList<String> val = new ArrayList<String>(1);
              val.add(vals[1]);
              map.put(key, val);
              stokentuple.add(map);
            }
            else { // For now do ArrayList
              ArrayList list = new ArrayList(vals.length);
              for (int i = 1; i < vals.length; i++) {
                list.add(vals[i]);
              }
              map.put(key, list);
              stokentuple.add(map);
            }
          }
        }
      }
      if (tokens.isConnected()) {
        tokens.emit(tokentuple);
      }
      if (splittokens.isConnected()) {
        splittokens.emit(stokentuple);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<ArrayList<String>> tokens = new DefaultOutputPort<ArrayList<String>>(this);
  @OutputPortFieldAnnotation(name = "splittokens")
  public final transient DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>> splittokens = new DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>>(this);
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
