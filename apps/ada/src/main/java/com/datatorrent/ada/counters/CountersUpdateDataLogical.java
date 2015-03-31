/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Map;


@DataType(type=CountersUpdateDataLogical.TYPE)
@DataDeserializerInfo(clazz=CountersUpdateDataLogicalDeserializer.class)
@DataValidatorInfo(clazz=CountersUpdateDataLogicalValidator.class)
public class CountersUpdateDataLogical extends CountersData
{
  public static final String TYPE = "logicalData";

  public static final String FIELD_WINDOW_ID = "windowID";
  public static final String FIELD_LOGICAL_COUNTERS = "logicalCounters";
  public static final String FIELD_APP_ID = "appID";
  public static final String FIELD_TIME = "time";

  @NotNull
  private Long windowID;
  @NotNull
  private Long time;
  @NotNull
  private String appID;

  @NotNull
  private Map<String, GPOImmutable> aggregateToVals;

  public CountersUpdateDataLogical()
  {
  }

  @JsonIgnore
  public DataGroup getDataGroup()
  {
    return new DataGroup(this.getUser(),
                         this.getAppName(),
                         this.getLogicalOperatorName(),
                         this.getVersion());
  }

  /**
   * @return the appID
   */
  public String getAppID()
  {
    return appID;
  }

  /**
   * @param appID the appID to set
   */
  public void setAppID(String appID)
  {
    this.appID = appID;
  }

  /**
   * @return the windowID
   */
  public Long getWindowID()
  {
    return windowID;
  }

  /**
   * @param windowID the windowID to set
   */
  public void setWindowID(Long windowID)
  {
    this.windowID = windowID;
  }

  /**
   * @return the counters
   */
  public Map<String, GPOImmutable> getAggregateToVals()
  {
    return aggregateToVals;
  }

  /**
   * @param aggregateToVals the counters to set
   */
  public void setAggregateToVals(Map<String, GPOImmutable> aggregateToVals)
  {
    this.aggregateToVals = aggregateToVals;
  }

  /**
   * @return the time
   */
  public Long getTime()
  {
    return time;
  }

  /**
   * @param time the time to set
   */
  public void setTime(Long time)
  {
    this.time = time;
  }
}
