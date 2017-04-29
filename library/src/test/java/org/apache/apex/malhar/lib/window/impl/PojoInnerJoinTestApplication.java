/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.window.impl;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.validation.constraints.Min;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.accumulation.PojoInnerJoin;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Example application to show usage of Windowed Merge Operator using PojoInnerJoin accumulation.
 */
public class PojoInnerJoinTestApplication implements StreamingApplication
{
  private static int records = 0;
  private static int SalesCount = 0;
  private static int ProductCount = 0;

  public static class POJOGenerator implements InputOperator
  {
    @Min(1)
    private int maxProductId = 1;
    @Min(1)
    private int maxCustomerId = 100000;
    @Min(1)
    int totalTuples;
    private int maxProductCategories = 100;
    private double maxAmount = 100.0;
    private long tuplesCounter;
    private long time;
    private long timeIncrement;
    private boolean isSalesEvent = true;

    // Limit number of emitted tuples per window
    @Min(0)
    private long maxTuplesPerWindow = 100;
    private final Random random = new Random();
    public final transient DefaultOutputPort<Tuple.WindowedTuple<SalesEvent>> outputsales = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<Tuple.WindowedTuple<ProductEvent>> outputproduct = new DefaultOutputPort<>();
    private static final long windowDuration = 1000;
    private static WindowedStorage.WindowedPlainStorage<WindowState> windowStateMap = new InMemoryWindowedStorage<>();
    public final transient DefaultOutputPort<ControlTuple> watermarkDefaultOutputPort = new DefaultOutputPort<>();

    private long watermarkTime;
    private long startingTime;

    public static Window.TimeWindow assignTestWindow(long timestamp)
    {
      long beginTimestamp = timestamp - timestamp % windowDuration;
      Window.TimeWindow window = new Window.TimeWindow(beginTimestamp, windowDuration);
      if (!windowStateMap.containsWindow(window)) {
        windowStateMap.put(window, new WindowState());
      }
      return window;
    }

    public POJOGenerator(int maxProductId, int totalTuples )
    {
      this.maxProductId = maxProductId;
      this.totalTuples = totalTuples;
    }

    public POJOGenerator()
    {
      //for kyro
    }

    @Override
    public void beginWindow(long l)
    {

    }

    @Override
    public void endWindow()
    {
      if (tuplesCounter < totalTuples) {
        watermarkDefaultOutputPort.emit(new WatermarkImpl(watermarkTime));
      }
      time += timeIncrement;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      //super.setup(context);
      startingTime = System.currentTimeMillis();
      watermarkTime = System.currentTimeMillis() + 10000;
      tuplesCounter = 0;
      time = System.currentTimeMillis();
      timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
        context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    }

    @Override
    public void teardown()
    {

    }

    SalesEvent generateSalesEvent() throws Exception
    {
      SalesEvent salesEvent = new SalesEvent();
      salesEvent.productId = randomId(maxProductId);
      salesEvent.customerId = randomId(maxCustomerId);
      salesEvent.amount = randomAmount();
      salesEvent.timestamps = time;
      return salesEvent;
    }

    ProductEvent generateProductEvent() throws Exception
    {
      ProductEvent productEvent = new ProductEvent();
      productEvent.productId = randomId(maxProductId);
      productEvent.productCategory = randomId(maxProductCategories);
      productEvent.timestamp = time;
      return productEvent;
    }

    private int randomId(int max)
    {
      if (max < 1) {
        return 1;
      }
      return 1 + random.nextInt(max);
    }

    private double randomAmount()
    {
      return maxAmount * random.nextDouble();
    }

    @Override
    public void emitTuples()
    {
      try {
        while (tuplesCounter < totalTuples) {
          //if (System.currentTimeMillis() - startingTime >= (i + 1) * 400) {
          if (isSalesEvent) {
            SalesEvent event = generateSalesEvent();
            this.outputsales.emit(new Tuple.WindowedTuple<SalesEvent>(assignTestWindow(System.currentTimeMillis()), event));
            SalesCount++;
          } else {
            ProductEvent event = generateProductEvent();
            this.outputproduct.emit(new Tuple.WindowedTuple<ProductEvent>(assignTestWindow(System.currentTimeMillis()), event));
            ProductCount++;
          }
          tuplesCounter++;
          //}
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public static class SalesEvent
    {
      public int customerId;
      public int productId;
      public int productCategory;
      public double amount;
      public long timestamps;

      public int getCustomerId()
      {
        return customerId;
      }

      public void setCustomerId(int customerId)
      {
        this.customerId = customerId;
      }

      public int getProductId()
      {
        return productId;
      }

      public void setProductId(int productId)
      {
        this.productId = productId;
      }

      public int getProductCategory()
      {
        return productCategory;
      }

      public void setProductCategory(int productCategory)
      {
        this.productCategory = productCategory;
      }

      public double getAmount()
      {
        return amount;
      }

      public void setAmount(double amount)
      {
        this.amount = amount;
      }

      public long getTimestamps()
      {
        return timestamps;
      }

      public void setTimestamps(long timestamp)
      {
        this.timestamps = timestamp;
      }
    }



    public static class ProductEvent
    {
      public int productId;
      public int productCategory;
      public long timestamp;

      public int getProductId()
      {
        return productId;
      }

      public void setProductId(int productId)
      {
        this.productId = productId;
      }

      public int getProductCategory()
      {
        return productCategory;
      }

      public void setProductCategory(int productCategory)
      {
        this.productCategory = productCategory;
      }

      public long getTimestamp()
      {
        return timestamp;
      }

      public void setTimestamp(long timestamp)
      {
        this.timestamp = timestamp;
      }
    }

    public static class OutputEvent
    {
      public int customerId;
      public int productId;
      public int productCategory;
      public long timestamp;
      public double amount;
      public long timestamps;

      public int getCustomerId()
      {
        return customerId;
      }

      public void setCustomerId(int customerId)
      {
        this.customerId = customerId;
      }

      public int getProductId()
      {
        return productId;
      }

      public void setProductId(int productId)
      {
        this.productId = productId;
      }

      public int getProductCategory()
      {
        return productCategory;
      }

      public void setProductCategory(int productCategory)
      {
        this.productCategory = productCategory;
      }

      public long getTimestamp()
      {
        return timestamp;
      }

      public void setTimestamp(long timestamp)
      {
        this.timestamp = timestamp;
      }

      public double getAmount()
      {
        return amount;
      }

      public void setAmount(double amount)
      {
        this.amount = amount;
      }

      public long getTimestamps()
      {
        return timestamps;
      }

      public void setTimestamps(long timestamp)
      {
        this.timestamps = timestamp;
      }
    }

    public int getMaxProductId()
    {
      return maxProductId;
    }

    public void setMaxProductId(int maxProductId)
    {
      this.maxProductId = maxProductId;
    }

    public int getMaxCustomerId()
    {
      return maxCustomerId;
    }

    public void setMaxCustomerId(int maxCustomerId)
    {
      this.maxCustomerId = maxCustomerId;
    }

    public int getMaxProductCategories()
    {
      return maxProductCategories;
    }

    public void setMaxProductCategories(int maxProductCategories)
    {
      this.maxProductCategories = maxProductCategories;
    }

    public double getMaxAmount()
    {
      return maxAmount;
    }

    public void setMaxAmount(double maxAmount)
    {
      this.maxAmount = maxAmount;
    }

    public boolean isSalesEvent()
    {
      return isSalesEvent;
    }

    public void setSalesEvent(boolean salesEvent)
    {
      isSalesEvent = salesEvent;
    }

    public long getMaxTuplesPerWindow()
    {
      return maxTuplesPerWindow;
    }

    public void setMaxTuplesPerWindow(long maxTuplesPerWindow)
    {
      this.maxTuplesPerWindow = maxTuplesPerWindow;
    }
  }

  public static class ResultCollector extends BaseOperator
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object t)
      {
        records++;
      }
    };

  }


  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator salesGenerator = dag.addOperator("Input1", new POJOGenerator(1,1));
    POJOGenerator productGenerator = dag.addOperator("Input2", new POJOGenerator(1,1));
    productGenerator.setSalesEvent(false);
    WindowedMergeOperatorImpl<POJOGenerator.SalesEvent, POJOGenerator.ProductEvent, List<Set<Object>>, List<List<Object>>> op
        = dag.addOperator("Merge", new WindowedMergeOperatorImpl<POJOGenerator.SalesEvent, POJOGenerator.ProductEvent, List<Set<Object>>, List<List<Object>>>());
    op.setAccumulation(new PojoInnerJoin(2, POJOGenerator.OutputEvent.class, "productId","productId"));
    op.setDataStorage(new InMemoryWindowedStorage<List<Set<Object>>>());

    WindowedStorage.WindowedPlainStorage<WindowState> windowStateMap = new InMemoryWindowedStorage<>();
    op.setWindowStateStorage(windowStateMap);
    op.setWindowOption(new WindowOption.TimeWindows(Duration.millis(10)));

    op.setTriggerOption(new TriggerOption().withEarlyFiringsAtEvery(1).accumulatingFiredPanes());
    op.setAllowedLateness(Duration.millis(500));
    ResultCollector results = dag.addOperator("results", new ResultCollector());


    dag.addStream("SalesToJoin", salesGenerator.outputsales, op.input);
    dag.addStream("ProductToJoin", productGenerator.outputproduct, op.input2);

    dag.addStream("results", op.output, results.input);
    dag.addStream("wm1", salesGenerator.watermarkDefaultOutputPort,op.controlInput);
    dag.addStream("wm2", productGenerator.watermarkDefaultOutputPort,op.controlInput2);

  }

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new PojoInnerJoinTestApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return SalesCount == 1 && ProductCount == 1 && records == 2;
      }
    });
    lc.run(20000);
    Assert.assertEquals(2,records);
  }
}
