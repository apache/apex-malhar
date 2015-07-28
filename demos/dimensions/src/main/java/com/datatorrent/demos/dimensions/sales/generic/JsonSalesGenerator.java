/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.InputGenerator;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.google.common.collect.Maps;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.Map;
import java.util.Random;

/**
 * Generates sales events data and sends them out as JSON encoded byte arrays.
 * <p>
 * Sales events are JSON string encoded as byte arrays. All id's are expected to be positive integers, and default to 1.
 * Transaction amounts are double values with two decimal places. Timestamp is unix epoch in milliseconds.
 * Product categories are not assigned by default. They are expected to be added by the Enrichment operator, but can be
 * enabled with addProductCategory override.
 *
 * Example Sales Event
 *
 * {
 *    "productId": 1,
 *    "customerId": 12345,
 *    "productCategory": 0,
 *    "regionId": 2,
 *    "channelId": 3,
 *    "sales": 107.99,
 *    "tax": 7.99,
 *    "discount": 15.73,
 *    "timestamp": 1412897574000
 * }
 *
 * @displayName JSON Sales Event Generator
 * @category Test Bench
 * @tags input, generator, json
 *
 * @since 2.0.0
 */
public class JsonSalesGenerator implements InputGenerator<byte[]>
{
  public static final String KEY_PRODUCT = "product";
  public static final String KEY_CUSTOMER = "customer";
  public static final String KEY_CHANNEL = "channel";
  public static final String KEY_REGION = "region";

  public static final String AGG_AMOUNT = "amount";
  public static final String AGG_DISCOUNT = "discount";
  public static final String AGG_TAX = "tax";

  private transient int maxProductId;
  private transient int maxCustomerId;
  private transient int maxChannelId;
  private transient int maxRegionId;

  private double minAmount = 0.99;
  private double maxAmount = 100.0;
  private double taxTiers[] = new double[] { 0.0, 0.04, 0.055, 0.0625, 0.0725, 0.085};
  private double discountTiers[] = new double[] {0.0, 0.025, 0.05, 0.10, 0.15, 0.50};
  private double maxDiscountPercent = 0.75;

  // Limit number of emitted tuples per window
  @Min(0)
  private long maxTuplesPerWindow = 300;

  // Maximum sales of deviation below the maximum tuples per window
  @Min(0)
  private int tuplesPerWindowDeviation = 200;

  // Number of windows to maintain the same deviation before selecting another
  @Min(1)
  private int tuplesRateCycle = 40;

  // Number of windows to maintain the same discount before selecting another
  @Min(1)
  private int discountCycle = 600;
  @Min(1)
  private int regionalCycle = 60;
  @Min(1)
  private int channelCycle = 60;

  @NotNull
  private String eventSchemaJSON;
  private transient DimensionalSchema schema;

  /**
   * Outputs sales event in JSON format as a byte array
   */
  public final transient DefaultOutputPort<byte[]> jsonBytes = new DefaultOutputPort<byte[]>();

  private static final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  private final Random random = new Random();

  private long tuplesCounter = 0;
  private long tuplesPerCurrentWindow = maxTuplesPerWindow;
  private transient Map<Integer, Double> channelDiscount = Maps.newHashMap();
  private transient Map<Integer, Double> regionalDiscount = Maps.newHashMap();
  private transient Map<Integer, Double> regionalTax = Maps.newHashMap();
  private transient RandomWeightedMovableGenerator<Integer> regionalGenerator = new RandomWeightedMovableGenerator<Integer>();
  private transient RandomWeightedMovableGenerator<Integer> channelGenerator = new RandomWeightedMovableGenerator<Integer>();


  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
    // Generate new output rate after tuplesRateCycle windows ONLY if tuplesPerWindowDeviation is non-zero
    if (windowId % tuplesRateCycle == 0 && tuplesPerWindowDeviation > 0) {
      tuplesPerCurrentWindow = maxTuplesPerWindow - random.nextInt(tuplesPerWindowDeviation);
    }

    // Generate new discounts after every discountCycle windows
    if ( windowId % discountCycle == 0) {
      generateDiscounts();
    }

    // Update channel selection probability (represents sales volume shifts between channels)
    if (windowId % channelCycle == 0) {
      channelGenerator.move();
    }

    // Update region selection probability (represents sales volume shifts between regions)
    if (windowId % regionalCycle == 0) {
      regionalGenerator.move();
    }

  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();

    schema = new DimensionalSchema(new DimensionalConfigurationSchema(eventSchemaJSON,
                                                              AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    maxProductId = 100;
    maxCustomerId = 30;
    maxChannelId = schema.getDimensionalConfigurationSchema().getKeysToEnumValuesList().get(KEY_CHANNEL).size();
    maxRegionId = schema.getDimensionalConfigurationSchema().getKeysToEnumValuesList().get(KEY_REGION).size();

    tuplesPerCurrentWindow = maxTuplesPerWindow;
    generateDiscounts();
    generateRegionalTax();
    initializeDataGenerators();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < tuplesPerCurrentWindow) {
      try {

        SalesEvent salesEvent = generateSalesEvent();
        this.jsonBytes.emit(mapper.writeValueAsBytes(salesEvent));

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Initialize regional and channel data generators
   */
  void initializeDataGenerators() {
    // Channels - with increased level of change
    channelGenerator.setMoveDeviation(channelGenerator.getMoveDeviation() * 2);
    for (int i=0; i < getMaxChannelId(); i++) {
      channelGenerator.add(i);
    }
    // Regions - default level of change
    for (int i=0; i < getMaxRegionId(); i++) {
      regionalGenerator.add(i);
    }
  }

  /**
   * Generate discounts per sales channel and region based on discount tiers
   */
  void generateDiscounts() {
    // Discounts per sales channel
    for (int i=0; i < getMaxChannelId(); i++) {
      // 50% chance of applying discount to a channel
      channelDiscount.put(i, (random.nextBoolean()) ? discountTiers[random.nextInt(discountTiers.length)] : 0.0);
    }
    // Discounts per region
    for (int i=0; i < getMaxRegionId(); i++) {
      // 50% chance of applying discount to a region
      regionalDiscount.put(i, (random.nextBoolean()) ? discountTiers[random.nextInt(discountTiers.length)] : 0.0);
    }
  }

  /**
   * Generate taxes based on each region
   */
  void generateRegionalTax() {
    for (int i=0; i < getMaxRegionId(); i++) {
      regionalTax.put(i, taxTiers[random.nextInt(taxTiers.length)]);
    }
  }

  SalesEvent generateSalesEvent() throws Exception {
    int regionId = regionalGenerator.next();
    int productId = randomId(maxProductId);
    int channelId = channelGenerator.next();

    SalesEvent salesEvent = new SalesEvent();
    salesEvent.time = System.currentTimeMillis();
    salesEvent.productId = productId;
    salesEvent.channel = (String) schema.getDimensionalConfigurationSchema().getKeysToEnumValuesList().get(KEY_CHANNEL).get(channelId);
    salesEvent.region = (String) schema.getDimensionalConfigurationSchema().getKeysToEnumValuesList().get(KEY_REGION).get(regionId);
    salesEvent.sales = randomAmount(minAmount, maxAmount);
    salesEvent.tax = calculateTax(salesEvent.sales, regionId);
    salesEvent.discount = calculateDiscount(salesEvent.sales, channelId, regionId);

    return salesEvent;
  }

  private int randomId(int max) {
    // Provide safe default for invalid max
    if (max < 1) return 1;
    return 1 + random.nextInt(max);
  }

  private int randomCustomerByRegion(int regionId) {
    int regionMultiplier = getMaxCustomerId() / getMaxRegionId();
    return (regionId * regionMultiplier) - random.nextInt(regionMultiplier);
  }

  private double calculateTax(double amount, int regionId) {
    double rawAmount =  amount * regionalTax.get(regionId);
    return Math.floor(rawAmount * 100) / 100;
  }

  private double calculateDiscount(double amount, int channelId, int regionId) {
    // Total discount is combination of channel and region discounts up to maximum allowed discount percentage
    double rawAmount = amount * Math.min(channelDiscount.get(channelId) + regionalDiscount.get(regionId), maxDiscountPercent);
    return Math.floor(rawAmount * 100) / 100;
  }

  // Generate random double with gaussian distribution between min and max and two decimal places
  private double randomAmount(double min, double max) {
    // Provide safe default for invalid min/max relationships
    if (max <= min) return Math.floor(min * 100) / 100;
    double mean = (min + max)/2.0;
    double deviation = (max - min)/2.0/3.0;
    double rawAmount;
    do {
      rawAmount = random.nextGaussian() * deviation + mean;
    } while (rawAmount < minAmount || rawAmount > maxAmount);
    return Math.floor(rawAmount * 100) / 100;
  }

  // Generate random tax given transaction sales
  private double randomPercent(double amount, double percent) {
    double tax = amount * ( random.nextDouble() * percent);
    return Math.floor(tax * 100) / 100;
  }


  public long getMaxTuplesPerWindow() {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public int getMaxProductId() {
    return maxProductId;
  }

  public int getMaxCustomerId() {
    return maxCustomerId;
  }

  public int getMaxChannelId() {
    return maxChannelId;
  }

  public double getMinAmount() {
    return minAmount;
  }

  public void setMinAmount(double minAmount) {
    this.minAmount = minAmount;
  }

  public double getMaxAmount() {
    return maxAmount;
  }

  public void setMaxAmount(double maxAmount) {
    this.maxAmount = maxAmount;
  }

  public int getTuplesPerWindowDeviation() {
    return tuplesPerWindowDeviation;
  }

  public void setTuplesPerWindowDeviation(int tuplesPerWindowDeviation) {
    this.tuplesPerWindowDeviation = tuplesPerWindowDeviation;
  }

  public int getTuplesRateCycle() {
    return tuplesRateCycle;
  }

  public void setTuplesRateCycle(int tuplesRateCycle) {
    this.tuplesRateCycle = tuplesRateCycle;
  }

  public int getMaxRegionId() {
    return maxRegionId;
  }

  public void setMaxRegionId(int maxRegionId) {
    if (maxRegionId >= 1)
      this.maxRegionId = maxRegionId;
  }

  public double getMaxDiscountPercent() {
    return maxDiscountPercent;
  }

  public void setMaxDiscountPercent(double maxDiscountPercent) {
    this.maxDiscountPercent = maxDiscountPercent;
  }

  public int getDiscountCycle() {
    return discountCycle;
  }

  public void setDiscountCycle(int discountCycle) {
    this.discountCycle = discountCycle;
  }

  /**
   * @return the eventSchemaJSON
   */
  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }

  @Override
  public OutputPort<byte[]> getOutputPort()
  {
    return jsonBytes;
  }
}

/**
 * A single sales event
 */
class SalesEvent {

  /* dimension keys */
  public long time;
  public int productId;
  public String customer;
  public String channel;
  public String region;
  /* metrics */
  public double sales;
  public double discount;
  public double tax;
}
