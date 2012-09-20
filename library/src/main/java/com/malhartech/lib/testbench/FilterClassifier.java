/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a in stream <b>in_data</b> and filters the tuples. Only sends out tuples as per filter numbers provided
 * on output port <b>out_data</b>. The aim is to create another stream representing a subsection of incoming load<p>
 * <br>
 * Examples of pairs include<br>
 * publisher,advertizer<br>
 * automobile,model<br>
 * <br>
 * The keys to be inserted are given by the property <b>keys</b>. Users can choose to insert their
 * own values via property <b>values</b>.<br>
 * For each incoming key users can provide an insertion
 * probability for the insert keys. This allows for randomization of the insert key choice<br><br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 10 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, Double> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>out_data</b>: Output port for emitting the new tuple<br>
 * <b>in_data</b>: Input port for receiving the incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the insert keys in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is for insertion into the <value> field in the tuple. also called "insert value". If not specified the incoming values are not changed<br>
 * <b>weights</b> are comma separated list of probability weights for each incoming key. For each incoming key the weights have to be provided. If this parameter is empty all the weights are even for all keys<br>
 * <b>percent<b>A number between 0 and 100. This is the percent of the time a new tuple is created. If say the number is 1, then a randomly selected 1 tuple out of 100 would create an output typle<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if not provided the incoming value is passed through<br>
 * <b>weights</b> if specified the format has to be "key1:val1,val2,...,valn;key2:val1,val2,...,valn;...", where n has to be
 * number of keys in parameter <b>keys</b>. If not specified all weights are equal<br>
 * <b>filter</b> The first number has to be less than the second and both have to be positive<br>
 * <br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = FilterClassifier.IPORT_IN_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = FilterClassifier.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class FilterClassifier extends AbstractModule
{
  public static final String IPORT_IN_DATA = "in_data";
  public static final String OPORT_OUT_DATA = "out_data";
  private static Logger LOG = LoggerFactory.getLogger(FilterClassifier.class);
  HashMap<String, Double> keys = new HashMap<String, Double>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  // One of inkeys (Key to weight hash) or noweight (even weight) would be not null
  HashMap<String, ArrayList<Integer>> inkeys = null;
  ArrayList<Integer> noweight = null;
  boolean hasvalues = false;

  int total_weight = 0;
  int pass_filter = 0;
  int total_filter = 0;
  private Random random = new Random();

  /**
   * keys are comma separated list of keys to insert to keys in in_data stream<p>
   * The out bound keys are in_data(key)<delimiter>key
   *
   */
  public static final String KEY_KEYS = "keys";
  /**
   * values are to be assigned to each key. The tuple thus is a newkey,newvalue
   * pair. The value field can either be empty in which case the value in in_data tuple is
   * passed through as is; or the value corresponding to key is inserted
   *
   */
  public static final String KEY_VALUES = "values";
  /**
   * The weights define the probability of each key being assigned to current
   * in_data tuple based on the in_data tuple key. The total of all weights is equal to 100%.
   * If weights are not specified then the append probability is equal.
   */
  public static final String KEY_WEIGHTS = "weights";

  /**
   * The filter numbers. Two numbers n1,n2, where n1is less than n2 and we send out n1 tuples out of n2 incoming
   * tuples<br>
   */
  public static final String KEY_FILTER = "filter";

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {

    boolean ret = true;

    String iwstr = config.get(KEY_WEIGHTS, "");
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);
    String fstr = config.get(KEY_FILTER);


    if (kstr.length == 0) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    else {
      LOG.debug(String.format("Number of keys are %d", kstr.length));
    }

    if (!iwstr.isEmpty()) { // if empty noweights would be used
      String[] wstr = iwstr.split(";");
      for (String s: wstr) { // Each wstr is in_key:val1,val2,valN where N = num of keys
        if (s.isEmpty()) {
          ret = false;
          throw new IllegalArgumentException("One of the keys in \"weights\" is empty");
        }
        else {
          String[] keywstrs = s.split(":");
          if (keywstrs.length != 2) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Property \"weights\" has a bad key \"%s\" (need two strings separated by ':')", s));
          }
          String[] kwstrs = keywstrs[1].split(","); // Keywstrs[0] is the in_key
          if (kwstrs.length != kstr.length) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of weights (%d) in \"%s\" does not match the number of keys (%d) in \"%s\"",
                                  kwstrs.length, keywstrs[1], kstr.length, config.get(KEY_KEYS, "")));
          }
          else { // Now you get weights for each key
            for (String ws: kwstrs) {
              try {
                Integer.parseInt(ws);
              }
              catch (NumberFormatException e) {
                ret = false;
                throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", ws));
              }
            }
          }
        }
      }
    }

    hasvalues = (vstr.length != 0);
    if (!hasvalues) {
      LOG.debug("values was not provided, so keys would have value of 0");
    }
    else {
      for (String s: vstr) {
        try {
          Double.parseDouble(s);
        }
        catch (NumberFormatException e) {
          ret = false;
          throw new IllegalArgumentException(String.format("Value string should be float(%s)", s));
        }
      }
    }

    if (hasvalues && (vstr.length != kstr.length)) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("Number of values (%d) does not match number of keys (%d)",
                            vstr.length, kstr.length));
    }

    if (fstr.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"filter\" is empty");
    }
    else {
      String[] twofstr = fstr.split(",");
      if (twofstr.length != 2) {
        ret = false;
        throw new IllegalArgumentException(String.format("Parameter \"filter\" has wrong format \"%s\"", fstr));
      }
      else {
        for (String ws: twofstr) {
          try {
            Integer.parseInt(ws);
          }
          catch (NumberFormatException e) {
            ret = false;
            throw new IllegalArgumentException(String.format("Filter string should be an integer(%s)", ws));
          }
        }
      }
    }
    return ret;
  }

  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }

    // example format for iwstr is "home:60,10,35;finance:10,75,15;sports:20,10,70;mail:50,15,35"
    String iwstr = config.get(KEY_WEIGHTS, "");
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);
    String[] fstr = config.getTrimmedStrings(KEY_FILTER);

    pass_filter = Integer.parseInt(fstr[0]);
    total_filter = Integer.parseInt(fstr[1]);

    if (!iwstr.isEmpty()) {
      String[] wstr = iwstr.split(";");
      inkeys = new HashMap<String, ArrayList<Integer>>();
      for (String ts: wstr) { // ts is top string as <key>:weight1,weight2,...
        String[] twostr = ts.split(":");
        String[] weights = twostr[1].split(",");
        ArrayList<Integer> alist = new ArrayList<Integer>();
        Integer wtotal = 0;
        for (String ws: weights) {
          alist.add(Integer.parseInt(ws));
          wtotal += Integer.parseInt(ws);
        }
        alist.add(wtotal);
        inkeys.put(twostr[0], alist);
      }
    }
    else {
      // noweight would be used for all in_keys
      noweight = new ArrayList<Integer>();
      for (String s: kstr) {
        noweight.add(100); // Even distribution
        total_weight += 100;
      }
      noweight.add(total_weight);
    }

    int i = 0;
    // First load up the keys and the index hash (wtostr_index) for randomization to work
    for (String s: kstr) {
      if (hasvalues) {
        keys.put(s, new Double(Double.parseDouble(vstr[i])));
      }
      else {
        keys.put(s, new Double(0.0));
      }
      wtostr_index.put(i, s);
      i += 1;
    }
    LOG.debug(String.format("\nSetting up node (%s)\nkeys(%s)\nninkey(%s)\nw(%s)\nv(%s)\nFor (%s), (%s), (%s)\n"
            , this.toString()
            , (keys == null) ? "null" : keys.toString()
            , (inkeys == null) ? "null" : inkeys.toString()
            , (noweight == null) ? "null" : noweight.toString()
            , hasvalues ? "null" : "not null"
            , config.get(KEY_WEIGHTS, ""), config.get(KEY_KEYS, ""),  config.get(KEY_VALUES, "")));
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    // TBD, payload can be either a String or a HashMap
    // Later on add String type to it as the throughput is high
    // The nodes later can split string and construct the HashMap if need be
    // Save I/O
    // For now only HashMap is supported
    //
    // tuple should be "inkey,key" and "value" pair

    // Check if the tuple has to be ignored

    int fval = random.nextInt(total_filter - 1);
    if (fval >= pass_filter) {
      return;
    }

    // Now insertion needs to be done
    for (Map.Entry<String, Double> e: ((HashMap<String, Double>)payload).entrySet()) {
      String[] twokeys = e.getKey().split(",");
      if (twokeys.length == 2) {
        String inkey = twokeys[1];
        ArrayList<Integer> alist = noweight;
        if (inkeys != null) {
          alist = inkeys.get(inkey);
        }
        // now alist are the weights
        int rval = random.nextInt(alist.get(alist.size() - 1));
        int j = 0;
        int wval = 0;
        for (Integer ew: alist) {
          wval += ew.intValue();
          if (wval >= rval) {
            break;
          }
          j++;
        }
        HashMap<String, Double> tuple = new HashMap<String, Double>();
        String key = wtostr_index.get(j); // the key
        Double keyval = null;
        if (hasvalues) {
          keyval = keys.get(key);
        }
        else { // pass on the value from incoming tuple
          keyval = e.getValue();
        }
        tuple.put(key + "," + inkey, keyval);
        emit(OPORT_OUT_DATA, tuple);
      }
    }
  }

  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
