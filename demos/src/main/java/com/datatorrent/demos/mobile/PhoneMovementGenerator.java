/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.mobile;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.HighLow;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in a stream via input port "data". Inverts the kindex and sends out the tuple on output port "kindex". Takes in specific queries on query port
 * and outputs the data in the cache through console port on receiving the tuple and on each subsequent end_of_window tuple<p>
 *
 * @author amol<br>
 *
 */
public class PhoneMovementGenerator extends BaseOperator
{
  private static Logger log = LoggerFactory.getLogger(PhoneMovementGenerator.class);

  //Need to check why setup is not being called
  //private transient SQLParser sqlParser = null;
  private SQLParser sqlParser = new SQLParser();

  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Integer> data = new DefaultInputPort<Integer>(this)
  {
    @Override
    public void process(Integer tuple)
    {
      HighLow loc = gps.get(tuple);
      if (loc == null) {
        loc = new HighLow(random.nextInt(range), random.nextInt(range));
        gps.put(tuple, loc);
      }
      int xloc = loc.getHigh().intValue();
      int yloc = loc.getLow().intValue();
      int state = rotate % 4;

      // Compute new location
      int delta = random.nextInt(100);
      if (delta >= threshold) {
        if (state < 2) {
          xloc++;
        }
        else {
          xloc--;
        }
        if (xloc < 0) {
          xloc += range;
        }
      }
      delta = random.nextInt(100);
      if (delta >= threshold) {
        if ((state == 1) || (state == 3)) {
          yloc++;
        }
        else {
          yloc--;
        }
        if (yloc < 0) {
          yloc += range;
        }
      }
      xloc = xloc % range;
      yloc = yloc % range;

      // Set new location
      HighLow nloc = newgps.get(tuple);
      if (nloc == null) {
        newgps.put(tuple, new HighLow(xloc, yloc));
      }
      else {
        nloc.setHigh(xloc);
        nloc.setLow(yloc);
      }
      rotate++;
    }
  };

  @InputPortFieldAnnotation(name = "query", optional=true)
  public final transient DefaultInputPort<Map<String, String>> locationQuery = new DefaultInputPort<Map<String, String>>(this)
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      log.info("new query: " + tuple);
      String qid = null;
      String phone = null;
      for (Map.Entry<String, String> e: tuple.entrySet()) {
        if (e.getKey().equals(KEY_QUERYID)) {
          qid = e.getValue();
        }
        else if (e.getKey().equals(KEY_PHONE)) {
          phone = e.getValue();
        }
      }

      if (qid != null) { // without qid, ignore
        if ((phone != null)) {
          if (phone.isEmpty()) {
            deregisterPhone(qid);
          }
          else {
            registerPhone(qid, phone);
          }
        }
      }
    }

  };

  public static final String KEY_PHONE = "phone";
  public static final String KEY_QUERYID = "queryId";
  public static final String KEY_LOCATION = "location";

  final HashMap<String, Integer> phone_register = new HashMap<String,Integer>();


  private final transient HashMap<Integer, HighLow> gps = new HashMap<Integer, HighLow>();
  private final Random random = new Random();
  private int range = 50;
  private int threshold = 80;
  private int rotate = 0;

  private final transient HashMap<Integer, HighLow> newgps = new HashMap<Integer, HighLow>();

  @Min(0)
  public int getRange()
  {
    return range;
  }

  public void setRange(int i)
  {
    range = i;
  }

  @Min(0)
  public int getThreshold()
  {
    return threshold;
  }

  public void setThreshold(int i)
  {
    threshold = i;
  }

  private void registerPhone(String qid, String phone) throws NumberFormatException
  {
    // register the phone channel
    phone_register.put(qid, new Integer(phone));
    log.debug(String.format("Registered query id \"%s\", with phonenum \"%s\"", qid, phone));
    emitQueryResult(qid, new Integer(phone));
  }

  private void deregisterPhone(String qid)
  {
    // simply remove the channel
    if (phone_register.get(qid) != null) {
      phone_register.remove(qid);
      log.debug(String.format("Removing query id \"%s\"", qid));
    }
  }

  public String getSql() {
    return "";
  }

  public void setSql(String query) {
    query = query.replace('+', ' ');
    SQLQuery sqlQuery = sqlParser.parseSQLQuery(query);
    if (sqlQuery != null) {
      if (sqlQuery instanceof SQLInsert) {
        SQLInsert sqlInsert = ((SQLInsert)sqlQuery);
        //int idx = 0;
        if (sqlInsert.getEntityName().equals("phone")) {
          List<String> names = sqlInsert.getNames();
          if ((names != null) && names.size() > 0) {
            String name = names.get(0);
            if (name.equals("number")) {
              List<List<String>> values = sqlInsert.getValues();
              for ( List<String> value : values ) {
                if (value.size() > 0) {
                  String phone = value.get(0);
                  //String qid = "" + System.currentTimeMillis() + "" + idx++;
                  //registerPhone(qid, phone);
                  registerPhone(phone, phone);
                }
              }
            }
          }
        }
      } else if (sqlQuery instanceof SQLDelete) {
        SQLDelete sqlDelete = (SQLDelete)sqlQuery;
        if (sqlDelete.getEntityName().equals("phone")) {
          String name = sqlDelete.getName();
          if (name != null) {
            if (name.equals("number")) {
              List<String> values = sqlDelete.getValues();
              for (String phone : values) {
                deregisterPhone(phone);
              }
            }
          }
        }
      }
    }
  }

  @OutputPortFieldAnnotation(name = "locationQueryResult")
  public final transient DefaultOutputPort<Map<String, String>> locationQueryResult = new DefaultOutputPort<Map<String, String>>(this);

  @Override
  public void setup(OperatorContext context)
  {
    //sqlParser = new SQLParser();
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<Integer, HighLow> e: newgps.entrySet()) {
      HighLow loc = gps.get(e.getKey());
      if (loc == null) {
        gps.put(e.getKey(), e.getValue());
      }
      else {
        loc.setHigh(e.getValue().getHigh());
        loc.setLow(e.getValue().getLow());
      }
    }
    boolean found = false;
    for (Map.Entry<String, Integer> p: phone_register.entrySet()) {
      emitQueryResult(p.getKey(), p.getValue());
      //log.debug(String.format("Query id is \"%s\", and phone is \"%d\"", p.getKey(), p.getValue()));
      found = true;
    }
    if (!found) {
      log.debug("No phone number");
    }
    newgps.clear();
  }

  private void emitQueryResult(String queryId, Integer phone) {
    HighLow loc = gps.get(phone);
    if (loc != null) {
      Map<String, String> queryResult = new HashMap<String, String>();
      queryResult.put(KEY_QUERYID, queryId);
      queryResult.put(KEY_PHONE, String.valueOf(phone));
      queryResult.put(KEY_LOCATION, loc.toString());
      locationQueryResult.emit(queryResult);
    }
  }


}
