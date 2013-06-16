package com.malhartech.demos.performance;

/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WordCountOperator<T> implements Operator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      count++;
    }

  };
  private transient ArrayList<Integer> counts;
  private transient int count;
  private long startmillis;
  private ArrayList<Integer> millis;

  @Override
  public void endWindow()
  {
    counts.add(count);
    millis.add((int)(System.currentTimeMillis() - startmillis));
    count = 0;

    if (counts.size() % 10 == 0) {
      logger.info("millis = {}", millis);
      logger.info("counts = {}", counts);
      millis.clear();
      counts.clear();
    }
  }

  @Override
  public void teardown()
  {
    logger.info("millis = {}", millis);
    logger.info("counts = {}", counts);
  }

  @Override
  public void beginWindow(long windowId)
  {
    startmillis = System.currentTimeMillis();
  }

  @Override
  public void setup(OperatorContext context)
  {
    counts = new ArrayList<Integer>();
    millis = new ArrayList<Integer>();
  }

  private static final long serialVersionUID = 201208061820L;
  private static final Logger logger = LoggerFactory.getLogger(WordCountOperator.class);
}
