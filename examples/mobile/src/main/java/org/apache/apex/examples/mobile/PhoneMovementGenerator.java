/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.mobile;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * This operator generates the GPS locations for the phone numbers specified.
 * The range of phone numbers or a specific phone number can be set for which the GPS locations will be generated.
 * It supports querying the locations of a given phone number.
 * This is a partionable operator that can partition as the tuplesBlast increases.
 * </p>
 *
 * @since 0.3.2
 */
public class PhoneMovementGenerator extends BaseOperator
{
  public final transient DefaultInputPort<Integer> data = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      HighLow<Integer> loc = gps.get(tuple);
      if (loc == null) {
        loc = new HighLow<Integer>(random.nextInt(range), random.nextInt(range));
        gps.put(tuple, loc);
      }
      int xloc = loc.getHigh();
      int yloc = loc.getLow();
      int state = rotate % 4;

      // Compute new location
      int delta = random.nextInt(100);
      if (delta >= threshold) {
        if (state < 2) {
          xloc++;
        } else {
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
        } else {
          yloc--;
        }
        if (yloc < 0) {
          yloc += range;
        }
      }
      xloc %= range;
      yloc %= range;

      // Set new location
      HighLow<Integer> nloc = newgps.get(tuple);
      if (nloc == null) {
        newgps.put(tuple, new HighLow<Integer>(xloc, yloc));
      } else {
        nloc.setHigh(xloc);
        nloc.setLow(yloc);
      }
      rotate++;
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Map<String,String>> phoneQuery = new DefaultInputPort<Map<String,String>>()
  {
    @Override
    public void process(Map<String,String> tuple)
    {
      LOG.info("new query {}", tuple);
      String command = tuple.get(KEY_COMMAND);
      if (command != null) {
        if (command.equals(COMMAND_ADD)) {
          commandCounters.getCounter(CommandCounters.ADD).increment();
          String phoneStr = tuple.get(KEY_PHONE);
          registerPhone(phoneStr);
        } else if (command.equals(COMMAND_ADD_RANGE)) {
          commandCounters.getCounter(CommandCounters.ADD_RANGE).increment();
          registerPhoneRange(tuple.get(KEY_START_PHONE), tuple.get(KEY_END_PHONE));
        } else if (command.equals(COMMAND_DELETE)) {
          commandCounters.getCounter(CommandCounters.DELETE).increment();
          String phoneStr = tuple.get(KEY_PHONE);
          deregisterPhone(phoneStr);
        } else if (command.equals(COMMAND_CLEAR)) {
          commandCounters.getCounter(CommandCounters.CLEAR).increment();
          clearPhones();
        }
      }
    }
  };

  public static final String KEY_COMMAND = "command";
  public static final String KEY_PHONE = "phone";
  public static final String KEY_LOCATION = "location";
  public static final String KEY_REMOVED = "removed";
  public static final String KEY_START_PHONE = "startPhone";
  public static final String KEY_END_PHONE = "endPhone";

  public static final String COMMAND_ADD = "add";
  public static final String COMMAND_ADD_RANGE = "addRange";
  public static final String COMMAND_DELETE = "del";
  public static final String COMMAND_CLEAR = "clear";

  final Set<Integer> phoneRegister = Sets.newHashSet();

  private final transient HashMap<Integer, HighLow<Integer>> gps = new HashMap<Integer, HighLow<Integer>>();
  private final Random random = new Random();
  private int range = 50;
  private int threshold = 80;
  private int rotate = 0;

  protected BasicCounters<MutableLong> commandCounters;

  private transient OperatorContext context;
  private final transient HashMap<Integer, HighLow<Integer>> newgps = new HashMap<Integer, HighLow<Integer>>();

  public PhoneMovementGenerator()
  {
    this.commandCounters = new BasicCounters<MutableLong>(MutableLong.class);
  }

  /**
   * @return the range of the phone numbers
   */
  @Min(0)
  public int getRange()
  {
    return range;
  }

  /**
   * Sets the range of phone numbers for which the GPS locations need to be generated.
   *
   * @param i the range of phone numbers to set
   */
  public void setRange(int i)
  {
    range = i;
  }

  /**
   * @return the threshold
   */
  @Min(0)
  public int getThreshold()
  {
    return threshold;
  }

  /**
   * Sets the threshold that decides how frequently the GPS locations are updated.
   *
   * @param i the value that decides how frequently the GPS locations change.
   */
  public void setThreshold(int i)
  {
    threshold = i;
  }

  private void registerPhone(String phoneStr)
  {
    // register the phone channel
    if (Strings.isNullOrEmpty(phoneStr)) {
      return;
    }
    try {
      Integer phone = new Integer(phoneStr);
      registerSinglePhone(phone);
    } catch (NumberFormatException nfe) {
      LOG.warn("Invalid no {}", phoneStr);
    }
  }

  private void registerPhoneRange(String startPhoneStr, String endPhoneStr)
  {
    if (Strings.isNullOrEmpty(startPhoneStr) || Strings.isNullOrEmpty(endPhoneStr)) {
      LOG.warn("Invalid phone range {} {}", startPhoneStr, endPhoneStr);
      return;
    }
    try {
      Integer startPhone = new Integer(startPhoneStr);
      Integer endPhone = new Integer(endPhoneStr);
      if (endPhone < startPhone) {
        LOG.warn("Invalid phone range {} {}", startPhone, endPhone);
        return;
      }
      for (int i = startPhone; i <= endPhone; i++) {
        registerSinglePhone(i);
      }
    } catch (NumberFormatException nfe) {
      LOG.warn("Invalid phone range <{},{}>", startPhoneStr, endPhoneStr);
    }
  }

  private void registerSinglePhone(int phone)
  {
    phoneRegister.add(phone);
    LOG.debug("Registered query id with phone {}", phone);
    emitQueryResult(phone);
  }

  private void deregisterPhone(String phoneStr)
  {
    if (Strings.isNullOrEmpty(phoneStr)) {
      return;
    }
    try {
      Integer phone = new Integer(phoneStr);
      // remove the channel
      if (phoneRegister.contains(phone)) {
        phoneRegister.remove(phone);
        LOG.debug("Removing query id {}", phone);
        emitPhoneRemoved(phone);
      }
    } catch (NumberFormatException nfe) {
      LOG.warn("Invalid phone {}", phoneStr);
    }
  }

  private void clearPhones()
  {
    phoneRegister.clear();
    LOG.info("Clearing phones");
  }

  public final transient DefaultOutputPort<Map<String, String>> locationQueryResult = new DefaultOutputPort<Map<String, String>>();

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    commandCounters.setCounter(CommandCounters.ADD, new MutableLong());
    commandCounters.setCounter(CommandCounters.ADD_RANGE, new MutableLong());
    commandCounters.setCounter(CommandCounters.DELETE, new MutableLong());
    commandCounters.setCounter(CommandCounters.CLEAR, new MutableLong());
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<Integer, HighLow<Integer>> e: newgps.entrySet()) {
      HighLow<Integer> loc = gps.get(e.getKey());
      if (loc == null) {
        gps.put(e.getKey(), e.getValue());
      } else {
        loc.setHigh(e.getValue().getHigh());
        loc.setLow(e.getValue().getLow());
      }
    }
    boolean found = false;
    for (Integer phone: phoneRegister) {
      emitQueryResult( phone);
      found = true;
    }
    if (!found) {
      LOG.debug("No phone number");
    }
    newgps.clear();
    context.setCounters(commandCounters);
  }

  private void emitQueryResult(Integer phone)
  {
    HighLow<Integer> loc = gps.get(phone);
    if (loc != null) {
      Map<String, String> queryResult = new HashMap<String, String>();
      queryResult.put(KEY_PHONE, String.valueOf(phone));
      queryResult.put(KEY_LOCATION, loc.toString());
      locationQueryResult.emit(queryResult);
    }
  }

  private void emitPhoneRemoved(Integer phone)
  {
    Map<String,String> removedResult = Maps.newHashMap();
    removedResult.put(KEY_PHONE, String.valueOf(phone));
    removedResult.put(KEY_REMOVED,"true");
    locationQueryResult.emit(removedResult);
  }

  public static enum CommandCounters
  {
    ADD, ADD_RANGE, DELETE, CLEAR
  }

  private static final Logger LOG = LoggerFactory.getLogger(PhoneMovementGenerator.class);
}
