package com.example.myapexapp;

import java.util.Random;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import javax.validation.ConstraintViolation;
import javax.validation.ValidatorFactory;
import javax.validation.Validator;
import javax.validation.Validation;

import com.datatorrent.api.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

/**
 * This is a simple operator that emits random integer.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(RandomNumberGenerator.class);

  @Min(1)
  private int numTuples = 20;
  private transient int count = 0;

  private int sleepTime;
  private transient long curWindowId;
  private transient Random random = new Random();

  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    long appWindowId = context.getValue(context.ACTIVATION_WINDOW_ID);
    sleepTime = context.getValue(context.SPIN_MILLIS);
    LOG.debug("Started setup, appWindowId = {}, sleepTime = {}", appWindowId, sleepTime);
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    LOG.debug("beginWindow: windowId = {}", windowId);
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(random.nextInt());
    } else {
      LOG.debug("count = {}, time = {}", count, System.currentTimeMillis());

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

}
