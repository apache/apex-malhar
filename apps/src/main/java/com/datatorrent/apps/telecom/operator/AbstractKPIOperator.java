package com.datatorrent.apps.telecom.operator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

public abstract class AbstractKPIOperator<K, V> implements Operator
{

  /**
   * This is the list of fields from the input tuple that are used to calculate KPI
   */
  @NotNull
  List<K> fieldsToCalculateKPI;

  /**
   * This is field in the input tuple that refers to the time stamp in tuple
   */
  @NotNull
  K timeField;

  /**
   * This specifies the dateFormat for the time field in the input tuple
   */
  @NotNull
  private DateFormat dateFormat;

  /**
   * This is the time interval for which the KPI is to be calculated. Currently it supports time intervals to be in
   * minutes. for e.g. 5m,10m,15m etc
   */
  @NotNull
  private int[] timeRange;

  /**
   * This field is used to store the minute received from the last tuple
   */
  private transient int lastMinute;

  /**
   * This is used to store the last minute for each time range
   */
  private int[] lastMinuteArray;

  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>() {
    @SuppressWarnings("deprecation")
    @Override
    public void process(Map<K, V> t)
    {
      try {
        lastMinute = (dateFormat.parse((String) t.get(timeField)).getDate() > lastMinute) ? dateFormat.parse((String) t.get(timeField)).getDate() : lastMinute;
      } catch (ParseException e) {
        logger.error("error while parsing the date {}", e.getMessage());
      }
      processInput(t);
    }
  };

  /**
   * This function needs to be implemented to handle the input tuples
   * 
   * @param t
   */
  public abstract void processInput(Map<K, V> t);

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
    for (int i = 0; i < timeRange.length; i++) {
      if (lastMinuteArray[i] != lastMinute && lastMinute % timeRange[i] == 1) { // instead of checking for 0, check is
                                                                                // made to 1
                                                                                // to ensure that all tuples for the
                                                                                // last minute have arrived
        emit(lastMinute - 1, timeRange[i]);
        lastMinuteArray[i] = lastMinute;
      }
      // if timeRange[i] == 1 it needs to be handled differently
      if (timeRange[i] == 1 && lastMinuteArray[i] != lastMinute) {
        emit(lastMinute - 1, timeRange[i]);
        lastMinuteArray[i] = lastMinute;
      }
    }
  }

  /**
   * 
   * @param minute
   * @param timeRange
   */
  public abstract void emit(int minute, int timeRange);

  public List<K> getFieldsToCalculateKPI()
  {
    return fieldsToCalculateKPI;
  }

  public void setFieldsToCalculateKPI(List<K> fieldsToCalculateKPI)
  {
    this.fieldsToCalculateKPI = fieldsToCalculateKPI;
  }

  public K getTimeField()
  {
    return timeField;
  }

  public void setTimeField(K timeField)
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

  public int[] getTimeRange()
  {
    return timeRange;
  }

  public void setTimeRange(int[] time)
  {
    this.timeRange = time;
    lastMinuteArray = new int[timeRange.length];
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractKPIOperator.class);
}
