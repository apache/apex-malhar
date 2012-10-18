package com.malhartech.demos.performance;

/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.Module;
import com.malhartech.dag.Component;
import com.malhartech.dag.OperatorConfiguration;
import com.malhartech.api.Sink;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = Component.INPUT, type = PortType.INPUT)
})
public class WordCountModule extends Module implements Sink
{
  private static final long serialVersionUID = 201208061820L;
  private static final Logger logger = LoggerFactory.getLogger(WordCountModule.class);

  ArrayList<Integer> counts = new ArrayList<Integer>();
  int count = 0;

  @Override
  public void endWindow()
  {
    counts.add(count);
    count = 0;

    if (counts.size() % 10 == 0) {
      logger.info("counts = {}", counts);
      counts.clear();
    }
  }

  @Override
  public void process(Object payload)
  {
    count++;
  }

  @Override
  public void teardown()
  {
    logger.info("counts = {}", counts);
  }
}
