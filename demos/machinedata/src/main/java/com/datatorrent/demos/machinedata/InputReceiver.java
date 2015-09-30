/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.machinedata.data.MachineInfo;
import com.datatorrent.demos.machinedata.data.MachineKey;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Information tuple generator with randomness.
 * </p>
 *
 * @since 0.3.5
 */
@SuppressWarnings("unused")
public class InputReceiver extends BaseOperator implements InputOperator
{
  public static final String CUSTOMER_ID = "Customer ID";
  public static final String PRODUCT_ID = "Product ID";
  public static final String PRODUCT_OS = "Product OS";
  public static final String SOFTWARE_VER_1 = "Software1 Ver";
  public static final String SOFTWARE_VER_2 = "Software2 Ver";
  public static final String DEVICE_ID = "Device ID";

  private final Random randomGen = new Random();

  private int tuplesPerWindow = 2000;
  private int tupleBlastSize = 1001;

  private transient int windowCount = 0;

  @NotNull
  private String eventSchema;
  private transient DimensionalConfigurationSchema schema;

  private static final DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
  private static final DateFormat dayDateFormat = new SimpleDateFormat("d");

  static {
    TimeZone tz = TimeZone.getTimeZone("GMT");
    minuteDateFormat.setTimeZone(tz);
    dayDateFormat.setTimeZone(tz);
  }

  public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
    schema = new DimensionalConfigurationSchema(eventSchema, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCount = 0;
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuples()
  {
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    String day = dayDateFormat.format(date);

    int count = Math.min(tuplesPerWindow - windowCount, tupleBlastSize);
    windowCount += count;

    for (; count > 0; count--) {
      randomGen.setSeed(System.currentTimeMillis());

      MachineKey machineKey = new MachineKey(date.getTime(), timeKey, day);

      machineKey.setCustomer(genCustomerId());
      machineKey.setProduct(genProductVer());
      machineKey.setOs(genOsVer());
      machineKey.setDeviceId(genDeviceId());
      machineKey.setSoftware1(genSoftware1Ver());
      machineKey.setSoftware2(genSoftware2Ver());
      MachineInfo machineInfo = new MachineInfo();
      machineInfo.setMachineKey(machineKey);
      machineInfo.setCpu(genCpu(calendar));
      machineInfo.setRam(genRam(calendar));
      machineInfo.setHdd(genHdd(calendar));

      outputInline.emit(machineInfo);
    }
  }

  private String genCustomerId()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(CUSTOMER_ID).size());
    return (String)schema.getKeysToEnumValuesList().get(CUSTOMER_ID).get(index);
  }

  private String genProductVer()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(PRODUCT_ID).size());
    return (String)schema.getKeysToEnumValuesList().get(PRODUCT_ID).get(index);
  }

  private String genOsVer()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(PRODUCT_OS).size());
    return (String)schema.getKeysToEnumValuesList().get(PRODUCT_OS).get(index);
  }

  private String genDeviceId()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(DEVICE_ID).size());
    return (String)schema.getKeysToEnumValuesList().get(DEVICE_ID).get(index);
  }

  private String genSoftware1Ver()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(SOFTWARE_VER_1).size());
    return (String)schema.getKeysToEnumValuesList().get(SOFTWARE_VER_1).get(index);
  }

  private String genSoftware2Ver()
  {
    int index = randomGen.nextInt(schema.getKeysToEnumValuesList().get(SOFTWARE_VER_2).size());
    return (String)schema.getKeysToEnumValuesList().get(SOFTWARE_VER_2).get(index);
  }

  private int genCpu(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute / 2 + 19;
    if (minute / 17 == 0) {
      second = cal.get(Calendar.SECOND);
      return (30 + randomGen.nextInt(range) + (minute % 7) - (second % 11));
    } else if (minute / 47 == 0) {
      second = cal.get(Calendar.SECOND);
      return (7 + randomGen.nextInt(range) + (minute % 7) - (second % 7));
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + (minute % 19) + (second % 7));
    }
  }

  private int genRam(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute + 1;
    if (minute / 23 == 0) {
      second = cal.get(Calendar.SECOND);
      return (20 + randomGen.nextInt(range) + (minute % 5) - (second % 11));
    } else if (minute / 37 == 0) {
      second = cal.get(Calendar.SECOND);
      return (11 + randomGen.nextInt(60) - (minute % 5) - (second % 11));
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + (minute % 17) + (second % 11));
    }
  }

  private int genHdd(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute / 2 + 1;
    if (minute / 37 == 0) {
      second = cal.get(Calendar.SECOND);
      return (25 + randomGen.nextInt(range) - minute % 7 - second % 11);
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + minute % 23 + second % 11);
    }
  }

  /**
   * Gets the event schema.
   *
   * @return The event schema.
   */
  public String getEventSchema()
  {
    return eventSchema;
  }

  /**
   * Sets the event schema.
   *
   * @param eventSchema The JSON event schema.
   */
  public void setEventSchema(String eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  /**
   * @return the tupleBlastSize
   */
  public int getTupleBlastSize()
  {
    return tupleBlastSize;
  }

  /**
   * Sets the number of tuples to emit in each call to emitTuples.
   *
   * @param tupleBlastSize The number of tuples to emit in each call to emitTuples.
   */
  public void setTupleBlastSize(int tupleBlastSize)
  {
    this.tupleBlastSize = tupleBlastSize;
  }

  /**
   * @return the tuplesPerWindow
   */
  public int getTuplesPerWindow()
  {
    return tuplesPerWindow;
  }

  /**
   * @param tuplesPerWindow the tuplesPerWindow to set
   */
  public void setTuplesPerWindow(int tuplesPerWindow)
  {
    this.tuplesPerWindow = tuplesPerWindow;
  }

  private static final Logger logger = LoggerFactory.getLogger(InputReceiver.class);
}
