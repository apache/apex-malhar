package com.example.transform;

import java.util.Date;
import java.util.Random;

import javax.validation.constraints.Min;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Generates and emits the CustomerEvent
 */
public class POJOGenerator implements InputOperator
{
  @Min(1)
  private int maxCustomerId = 100000;
  @Min(1)
  private int maxNameLength = 10;
  @Min(1)
  private int maxAddressLength = 15;
  private long tuplesCounter;
  // Limit number of emitted tuples per window
  @Min(1)
  private long maxTuplesPerWindow = 100;
  private final Random random = new Random();
  private final RandomStringUtils rRandom = new RandomStringUtils();
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {

  }

  CustomerEvent generateCustomersEvent() throws Exception {

    CustomerEvent customerEvent = new CustomerEvent();
    customerEvent.setCustomerId(randomId(maxCustomerId));
    customerEvent.setFirstName(rRandom.randomAlphabetic(randomId(maxNameLength)));
    customerEvent.setLastName(rRandom.randomAlphabetic(randomId(maxNameLength)));
    long val = random.nextLong();
    long diff1 = val % System.currentTimeMillis();
    customerEvent.setDateOfBirth(new Date(diff1));
    customerEvent.setAddress(rRandom.randomAlphabetic(randomId(maxAddressLength)));
    return customerEvent;
  }

  private int randomId(int max) {
    if (max < 1) return 1;
    return 1 + random.nextInt(max);
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < maxTuplesPerWindow) {
      try {
        CustomerEvent event = generateCustomersEvent();
        this.output.emit(event);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public long getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public int getMaxAddressLength()
  {
    return maxAddressLength;
  }

  public void setMaxAddressLength(int maxAddressLength)
  {
    this.maxAddressLength = maxAddressLength;
  }

  public int getMaxNameLength()
  {
    return maxNameLength;
  }

  public void setMaxNameLength(int maxNameLength)
  {
    this.maxNameLength = maxNameLength;
  }

  public int getMaxCustomerId()
  {
    return maxCustomerId;
  }

  public void setMaxCustomerId(int maxCustomerId)
  {
    this.maxCustomerId = maxCustomerId;
  }
}
