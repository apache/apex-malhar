package com.datatorrent.apps.telecom.operator;

import java.util.Map;
import java.util.Properties;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class EnrichmentOperator implements Operator
{

  /**
   * The concrete class that will enrich the incoming tuple
   */
  @NotNull
  private Class<? extends EnricherInterface> enricher;
  /**
   * The properties that need to be configure enricher
   */
  @NotNull
  private Properties prop;
  
  private transient EnricherInterface enricherObj;
  
  public final transient DefaultOutputPort<Map<String, String>> output = new DefaultOutputPort<Map<String, String>>();
  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>() {
    @Override
    public void process(Map<String, String> t)
    {
      enricherObj.enrichRecord(t);
      output.emit(t);      
    }
  };
  
  @Override
  public void setup(OperatorContext context)
  {
    try {
      enricherObj = enricher.newInstance();
    } catch (Exception e) {
      logger.info("can't instantiate object {}", e.getMessage());
      throw new RuntimeException("setup failed");
    } 
    enricherObj.configure(prop);
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
    
  }

  public Class<? extends EnricherInterface> getEnricher()
  {
    return enricher;
  }

  public void setEnricher(Class<? extends EnricherInterface> enricher)
  {
    this.enricher = enricher;
  }

  public Properties getProp()
  {
    return prop;
  }

  public void setProp(Properties prop)
  {
    this.prop = prop;
  }

  private static final Logger logger = LoggerFactory.getLogger(EnrichmentOperator.class);
  
}
