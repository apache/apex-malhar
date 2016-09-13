package com.example.join;

import java.util.Random;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Generates and emits the SalesEvent/ProductEvent based on isSalesEvent.
 */
public class POJOGenerator implements InputOperator
{
  @Min(1)
  private int maxProductId = 100000;
  @Min(1)
  private int maxCustomerId = 100000;
  @Min(1)
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
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
  }

  @Override
  public void endWindow()
  {
    time += timeIncrement;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    time = System.currentTimeMillis();
    timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
  }

  @Override
  public void teardown()
  {

  }

  SalesEvent generateSalesEvent() throws Exception {

    SalesEvent salesEvent = new SalesEvent();
    salesEvent.productId = randomId(maxProductId);
    salesEvent.customerId = randomId(maxCustomerId);
    salesEvent.amount = randomAmount();
    salesEvent.timestamp = time;
    return salesEvent;
  }

  ProductEvent generateProductEvent() throws Exception {
    ProductEvent productEvent = new ProductEvent();
    productEvent.productId = randomId(maxProductId);
    productEvent.productCategory = randomId(maxProductCategories);
    productEvent.timestamp = time;
    return productEvent;
  }

  private int randomId(int max) {
    if (max < 1) return 1;
    return 1 + random.nextInt(max);
  }

  private double randomAmount() {
    return maxAmount * random.nextDouble();
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < maxTuplesPerWindow) {
      try {
        if (isSalesEvent) {
          SalesEvent event = generateSalesEvent();
          this.output.emit(event);
        } else {
          ProductEvent event = generateProductEvent();
          this.output.emit(event);
        }

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static class SalesEvent
  {
    public int customerId;
    public int productId;
    public int productCategory;
    public double amount;
    public long timestamp;

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

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
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
