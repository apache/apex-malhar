package com.datatorrent.demos.mobile;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;
import java.util.Map;
import java.util.Random;

/**
 * Generates mobile numbers that will be displayed in mobile demo just after launch.<br></br>
 * Operator attributes:<b>
 * <ul>
 *   <li>initialDisplayCount: No. of seed phone numbers that will be generated.</li>
 *   <li>maxSeedPhoneNumber: The largest seed phone number.</li>
 * </ul>
 * </b>
 *
 * @since 0.3.5
 */
public class PhoneEntryOperator extends BaseOperator
{
  private static Logger LOG = LoggerFactory.getLogger(PhoneEntryOperator.class);

  private boolean seedGenerationDone = false;

  @Min(0)
  private int initialDisplayCount = 0;

  private int maxSeedPhoneNumber = 0;
  private int rangeLowerEndpoint;
  private int rangeUpperEndpoint;

  public void setInitialDisplayCount(int i)
  {
    initialDisplayCount = i;
  }

  public void setPhoneRange(Range<Integer> phoneRange)
  {
    this.rangeLowerEndpoint = phoneRange.lowerEndpoint();
    this.rangeUpperEndpoint = phoneRange.upperEndpoint();
  }

  public void setMaxSeedPhoneNumber(int number)
  {
    this.maxSeedPhoneNumber = number;
  }

  @InputPortFieldAnnotation(name = "query", optional = true)
  public final transient DefaultInputPort<Map<String, String>> locationQuery = new DefaultInputPort<Map<String, String>>()
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      seedPhones.emit(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "seedPhones")
  public final transient DefaultOutputPort<Map<String, String>> seedPhones = new DefaultOutputPort<Map<String, String>>();

  @Override
  public void beginWindow(long windowId){
    if (!seedGenerationDone) {
      Random random = new Random();
      int maxPhone = (maxSeedPhoneNumber <= rangeUpperEndpoint && maxSeedPhoneNumber >= rangeLowerEndpoint) ? maxSeedPhoneNumber : rangeUpperEndpoint;
      maxPhone -= 5550000;
      int phonesToDisplay = initialDisplayCount > maxPhone ? maxPhone : initialDisplayCount;
      for (int i = phonesToDisplay; i-- > 0; ) {
        int phoneNo = 5550000 + random.nextInt(maxPhone + 1);
        LOG.info("seed no: " + phoneNo);
        Map<String, String> valueMap = Maps.newHashMap();
        valueMap.put(PhoneMovementGenerator.KEY_COMMAND, PhoneMovementGenerator.COMMAND_ADD);
        valueMap.put(PhoneMovementGenerator.KEY_PHONE, Integer.toString(phoneNo));
        seedPhones.emit(valueMap);
      }
      // done generating data
      seedGenerationDone = true;
      LOG.info("Finished generating seed data.");
    }
  }
}
