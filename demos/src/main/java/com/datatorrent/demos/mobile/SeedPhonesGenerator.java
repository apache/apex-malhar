package com.datatorrent.demos.mobile;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;
import java.util.Map;
import java.util.Random;

/**
 * Generates mobile numbers that will be displayed in mobile demo just after launch.<br></br>
 * @since 0.3.5
 */
public class SeedPhonesGenerator extends BaseOperator implements InputOperator
{
  private static Logger LOG = LoggerFactory.getLogger(SeedPhonesGenerator.class);

  @Min(0)
  private int initialDisplayCount=0;

  public void setInitialDisplayCount(int i)
  {
    initialDisplayCount=i;
  }

  @OutputPortFieldAnnotation(name="seedPhones")
  public final transient DefaultOutputPort<Map<String,String>> seedPhones= new DefaultOutputPort<Map<String,String>>();

  @Override
  public void emitTuples()
  {
    Random random = new Random();
    for(int i=initialDisplayCount; i-- >0;)
    {
      int phoneNo= 5550000 + random.nextInt(10000);
      Map<String,String> query = Maps.newHashMap();
      query.put(PhoneMovementGenerator.KEY_COMMAND, PhoneMovementGenerator.COMMAND_ADD);
      query.put(PhoneMovementGenerator.KEY_PHONE, Integer.toString(phoneNo));
      query.put(PhoneMovementGenerator.KEY_QUERYID, "SPG"+i);
      seedPhones.emit(query);
    }
    // done generating data
    LOG.info("Finished generating data.");
    throw new RuntimeException(new InterruptedException("Finished generating data."));
  }
}
