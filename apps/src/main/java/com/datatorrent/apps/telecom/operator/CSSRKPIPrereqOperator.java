/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.telecom.operator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;

/**
 * <p>
 * AbstractKPIOperator class.
 * </p>
 * 
 * @since 0.9.2
 */
public class CSSRKPIPrereqOperator implements Operator
{

  /**
   * This is the list of fields from the input tuple that are used to calculate KPI
   */
  @NotNull
  List<String> fieldsToCalculateKPI;

  /**
   * This is field in the input tuple that refers to the time stamp in tuple
   */
  @NotNull
  String timeField;

  /**
   * This specifies the dateFormat for the time field in the input tuple
   */
  @NotNull
  DateFormat dateFormat;

  /**
   * This field is used to store the minute received from the last tuple
   */
  private Date lastMinute;

  private Map<TimeBucketKey, AverageData> dataMap = new HashMap<TimeBucketKey, AverageData>();

  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>() {
    @SuppressWarnings("deprecation")
    @Override
    public void process(Map<String, Object> t)
    {
      Date date = null;
      try {
        date = dateFormat.parse(t.get(timeField).toString());
        if (lastMinute == null) {
          lastMinute = date;
        } 
        if (date.compareTo(lastMinute) > 0 && date.getMinutes() != lastMinute.getMinutes()) { // the data is in
                                                                                              // the order and the new tuple has different minute
          emit(lastMinute);
          lastMinute = date;        
        } else if (date.compareTo(lastMinute) < 0) { // data is out of order
          processInput(t, date);
          emit(date);
          return;
        }
      } catch (ParseException e) {
        logger.error("error while parsing the date {}", e.getMessage());
      }
      processInput(t, date);
    }
  };

  public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, AverageData>> output = new DefaultOutputPort<KeyValPair<TimeBucketKey, AverageData>>() {
    public Unifier<KeyValPair<TimeBucketKey, AverageData>> getUnifier()
    {
      CSSRKPIUnifier unifier = new CSSRKPIUnifier();
      return unifier;
    }
  };

  private void emit(Date lastMinute)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(lastMinute);
    TimeBucketKey timeBucketKey = new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC);
    output.emit(new KeyValPair<TimeBucketKey, AverageData>(timeBucketKey, dataMap.get(timeBucketKey)));
    dataMap.remove(timeBucketKey);
  }

  /**
   * This function needs to be implemented to handle the input tuples
   * 
   * @param t
   */
  public void processInput(Map<String, Object> t, Date date)
  {
    AverageData average;
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    TimeBucketKey timeBucketKey = new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC);
    if (dataMap.get(timeBucketKey) == null) {
      average = new AverageData(0, 0);
      dataMap.put(timeBucketKey, average);
    } else {
      average = dataMap.get(timeBucketKey);
    }
    if (t.get(fieldsToCalculateKPI.get(0)) != null) {
      average.setSum(average.getSum() + 1);
    }
    if (t.get(fieldsToCalculateKPI.get(1)) != null) {
      average.setCount(average.getCount() + 1);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  public List<String> getFieldsToCalculateKPI()
  {
    return fieldsToCalculateKPI;
  }

  public void setFieldsToCalculateKPI(List<String> fieldsToCalculateKPI)
  {
    this.fieldsToCalculateKPI = fieldsToCalculateKPI;
  }

  public String getTimeField()
  {
    return timeField;
  }

  public void setTimeField(String timeField)
  {
    this.timeField = timeField;
  }

  public void setDateFormat(String dateFormat)
  {
    this.dateFormat = new SimpleDateFormat(dateFormat);
  }

  public DateFormat getDateFormat()
  {
    return dateFormat;
  }

  public void setDateFormat(DateFormat dateFormat)
  {
    this.dateFormat = dateFormat;
  }

  private static final Logger logger = LoggerFactory.getLogger(CSSRKPIPrereqOperator.class);
}
