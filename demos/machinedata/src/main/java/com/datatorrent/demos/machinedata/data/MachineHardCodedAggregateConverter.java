/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.demos.machinedata.data.MachineKey.KeySelector;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;

/**
 * @since 3.2.0
 */
public class MachineHardCodedAggregateConverter implements Operator, Partitioner<MachineHardCodedAggregateConverter>
{
  public static final int PARTITION_COUNT = 2;

  @NotNull
  private String configurationSchemaJSON;

  private final AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  private transient DimensionalConfigurationSchema configurationSchema;

  private transient int sumID;
  private transient int countID;
  private transient int timeBucket;

  private transient Int2ObjectOpenHashMap<KeySelector> ddIDToKeySelector = new Int2ObjectOpenHashMap<>();

  public final transient DefaultInputPort<MachineHardCodedAggregate> input = new DefaultInputPort<MachineHardCodedAggregate>()
  {
    @Override
    public void process(MachineHardCodedAggregate t)
    {
      int ddID = t.getDdID();
      int aggregatorID;

      if (t.isSum()) {
        aggregatorID = sumID;
      } else {
        aggregatorID = countID;
      }

      KeySelector ks = ddIDToKeySelector.get(ddID);

      FieldsDescriptor keyFieldsDescriptor = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(ddID);
      FieldsDescriptor aggregateFieldsDescriptor = configurationSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(ddID).get(aggregatorID);

      GPOMutable key = new GPOMutable(keyFieldsDescriptor);

      int keyIndex = 0;

      if (ks.useCustomer) {
        key.getFieldsString()[keyIndex] = t.customer;
        keyIndex++;
      }

      if (ks.useDeviceId) {
        key.getFieldsString()[keyIndex] = t.deviceId;
        keyIndex++;
      }

      if (ks.useProduct) {
        key.getFieldsString()[keyIndex] = t.product;
        keyIndex++;
      }

      if (ks.useOs) {
        key.getFieldsString()[keyIndex] = t.os;
        keyIndex++;
      }

      if (ks.useSoftware1) {
        key.getFieldsString()[keyIndex] = t.software1;
        keyIndex++;
      }

      if (ks.useSoftware2) {
        key.getFieldsString()[keyIndex] = t.software2;
        keyIndex++;
      }

      key.getFieldsInteger()[0] = timeBucket;
      key.getFieldsLong()[0] = t.timestamp;

      GPOMutable aggregate = new GPOMutable(aggregateFieldsDescriptor);

      @SuppressWarnings("MismatchedReadAndWriteOfArray")
      long[] fields = aggregate.getFieldsLong();

      if (t.isSum()) {
        fields[0] = t.cpuUsage;
        fields[1] = t.hddUsage;
        fields[2] = t.ramUsage;
      } else {
        fields[0] = fields[1] = fields[2] = t.count;
      }

      EventKey eventKey = new EventKey((int)AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                       AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID,
                                       ddID,
                                       aggregatorID,
                                       key);

      output.emit(new Aggregate(eventKey,
                                aggregate));
    }

    @Override
    public StreamCodec<MachineHardCodedAggregate> getStreamCodec()
    {
      return null;
    }

  };

  public final transient DefaultOutputPort<Aggregate> output = new DefaultOutputPort<>();

  public MachineHardCodedAggregateConverter()
  {
  }

  /**
   * @return the configurationSchemaJSON
   */
  public String getConfigurationSchemaJSON()
  {
    return configurationSchemaJSON;
  }

  /**
   * @param configurationSchemaJSON the configurationSchemaJSON to set
   */
  public void setConfigurationSchemaJSON(String configurationSchemaJSON)
  {
    this.configurationSchemaJSON = configurationSchemaJSON;
  }

  @Override
  public void setup(OperatorContext cntxt)
  {
    aggregatorRegistry.setup();
    this.configurationSchema = new DimensionalConfigurationSchema(configurationSchemaJSON,
                                                                  aggregatorRegistry);

    sumID = aggregatorRegistry.getIncrementalAggregatorNameToID().get("SUM");
    countID = aggregatorRegistry.getIncrementalAggregatorNameToID().get("COUNT");
    timeBucket = configurationSchema.getCustomTimeBucketRegistry().getTimeBucketId(new CustomTimeBucket(TimeBucket.MINUTE));

    for (Map.Entry<DimensionsDescriptor, Integer> entry : configurationSchema.getDimensionsDescriptorToID().entrySet()) {
      DimensionsDescriptor dimensionsDescriptor = entry.getKey();
      Integer ddID = entry.getValue();

      ddIDToKeySelector.put(ddID, new KeySelector(dimensionsDescriptor.getFields().getFields()));
    }
  }

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public Collection<Partition<MachineHardCodedAggregateConverter>> definePartitions(Collection<Partition<MachineHardCodedAggregateConverter>> clctn, PartitioningContext pc)
  {

    Kryo kryo = new Kryo();

    Collection<Partition<MachineHardCodedAggregateConverter>> partitions = Lists.newArrayList();

    MachineHardCodedAggregateConverter base;

    try {
      base = clone(kryo, clctn.iterator().next().getPartitionedInstance());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    for (int partitionCounter = 0; partitionCounter < PARTITION_COUNT; partitionCounter++) {
      MachineHardCodedAggregateConverter clonedOperator;

      try {
        clonedOperator = clone(kryo, base);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      Map<InputPort<?>, PartitionKeys> inputToPartitionKeys = Maps.newHashMap();

      for (InputPort<?> inputPort : pc.getInputPorts()) {
        inputToPartitionKeys.put(inputPort, new PartitionKeys(1, Sets.newHashSet(partitionCounter)));
      }

      Partition<MachineHardCodedAggregateConverter> partition = new DefaultPartition<>(clonedOperator);
      partition.getPartitionKeys().putAll(inputToPartitionKeys);
      partitions.add(partition);
    }

    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<MachineHardCodedAggregateConverter>> map)
  {
  }

  public static <T> T clone(Kryo kryo, T src) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, src);
    }
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }

  public static class ConverterStreamCodec extends DefaultStatefulStreamCodec<MachineHardCodedAggregate>
  {
    @Override
    public int getPartition(MachineHardCodedAggregate aggregate)
    {
      return aggregate.ddID % 2;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(MachineHardCodedAggregateConverter.class);
}
