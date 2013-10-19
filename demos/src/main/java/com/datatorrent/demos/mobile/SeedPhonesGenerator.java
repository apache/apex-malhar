package com.datatorrent.demos.mobile;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
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
 * @since 0.3.5
 */
public class SeedPhonesGenerator extends BaseOperator implements InputOperator
{
  private static Logger LOG = LoggerFactory.getLogger(SeedPhonesGenerator.class);

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

  @OutputPortFieldAnnotation(name = "seedPhones")
  public final transient DefaultOutputPort<Map<String, String>> seedPhones = new DefaultOutputPort<Map<String, String>>();

  @Override
  public void emitTuples()
  {
    Random random = new Random();
    int maxPhone = (maxSeedPhoneNumber <= rangeUpperEndpoint && maxSeedPhoneNumber >= rangeLowerEndpoint) ? maxSeedPhoneNumber : rangeUpperEndpoint;
    maxPhone -= 5550000;
    int phonesToDisplay = initialDisplayCount > maxPhone ? maxPhone : initialDisplayCount;
    for (int i = phonesToDisplay; i-- > 0; ) {
      int phoneNo = 5550000 + random.nextInt(maxPhone + 1);
      LOG.info("seed no: "+phoneNo);
      Map<String, String> query = Maps.newHashMap();
      query.put(PhoneMovementGenerator.KEY_COMMAND, PhoneMovementGenerator.COMMAND_ADD);
      query.put(PhoneMovementGenerator.KEY_PHONE, Integer.toString(phoneNo));
      query.put(PhoneMovementGenerator.KEY_QUERYID, "SPG" + i);
      seedPhones.emit(query);
    }
    // done generating data
    LOG.info("Finished generating data.");
    throw new RuntimeException(new InterruptedException("Finished generating data."));
  }
}
