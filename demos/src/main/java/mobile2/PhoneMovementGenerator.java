/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package mobile2;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.demos.mobile.InvertIndexMapPhone;
import com.malhartech.lib.util.HighLow;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
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
  private static Logger log = LoggerFactory.getLogger(InvertIndexMapPhone.class);

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
  public final transient DefaultInputPort<Map<String, String>> query = new DefaultInputPort<Map<String, String>>(this)
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      log.info("new query: " + tuple);
      String qid = null;
      String phone = null;
      String location = null;
      for (Map.Entry<String, String> e: tuple.entrySet()) {
        if (e.getKey().equals(IDENTIFIER_CHANNEL)) {
          qid = e.getValue();
        }
        else if (e.getKey().equals(CHANNEL_PHONE)) {
          phone = e.getValue();
        }
      }

      if (qid != null) { // without qid, ignore
        if ((phone != null)) {
          if (phone.isEmpty()) { // simply remove the channel
            if (phone_register.get(qid) != null) {
              phone_register.remove(qid);
              log.debug(String.format("Removing query id \"%s\"", qid));
            }
          }
          else { // register the phone channel
            phone_register.put(qid, new Integer(phone));
            log.debug(String.format("Registered query id \"%s\", with phonenum \"%s\"", qid, phone));
          }
        }
      }
    }
  };

  public static final String CHANNEL_PHONE = "phone";
  public static final String IDENTIFIER_CHANNEL = "queryId";

  private final transient HashMap<String, Integer> phone_register = new HashMap<String,Integer>();


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

  @OutputPortFieldAnnotation(name = "console")
  public final transient DefaultOutputPort<KeyValPair<Integer, HighLow>> locations = new DefaultOutputPort<KeyValPair<Integer, HighLow>>(this);

  @Override
  public void setup(OperatorContext context)
  {
    // Temporary for testing, remove once http query works
    phone_register.put("q1", 9994995);
    phone_register.put("q3", 9996101);
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
    for (Map.Entry<String, Integer> p: phone_register.entrySet()) {
      HighLow loc = gps.get(p.getValue());
      if (loc != null) {
        KeyValPair<Integer, HighLow> tuple = new KeyValPair<Integer, HighLow>(p.getValue(), new HighLow(loc.getHigh(), loc.getLow()));
        locations.emit(tuple);
      }
    }
    newgps.clear();
  }
}
