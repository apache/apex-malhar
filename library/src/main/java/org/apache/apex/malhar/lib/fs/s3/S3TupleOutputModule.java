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

package org.apache.apex.malhar.lib.fs.s3;

import java.util.Arrays;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.converter.Converter;
import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.NoOpConverter;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringToBytesConverter;
import org.apache.apex.malhar.lib.partitioner.StatelessThroughputBasedPartitioner;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.StatsListener;

/**
 * S3TupleOutputModule writes incoming tuples into files and uploads these files on Amazon S3.
 *
 * @param <INPUT> Type of incoming Tuple.Converter needs to be defined which converts these tuples to byte[].
 * Default converters for String, byte[] tuples are provided in
 * S3TupleOutputModule.S3BytesOutputModule, S3TupleOutputModule.S3StringOutputModule
 *
 * @displayName S3 Tuple Output Module
 * @tags S3, Output
 *
 * @since 3.7.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class S3TupleOutputModule<INPUT> implements Module
{
  public final transient ProxyInputPort<INPUT> input = new ProxyInputPort<INPUT>();
  public final transient ProxyOutputPort<FSRecordCompactionOperator.OutputMetaData> output = new ProxyOutputPort<>();

  /**
   * AWS access key
   */
  @NotNull
  private String accessKey;
  /**
   * AWS secret access key
   */
  @NotNull
  private String secretAccessKey;

  /**
   * S3 Region
   */
  private String region;
  /**
   * Name of the bucket in which to upload the files
   */
  @NotNull
  private String bucketName;

  /**
   * Path of the output directory. Relative path of the files copied will be
   * maintained w.r.t. source directory and output directory
   */
  @NotNull
  private String outputDirectoryPath;

  /**
   * Max number of idle windows for which no new data is added to current part
   * file. Part file will be finalized after these many idle windows after last
   * new data.
   */
  private long maxIdleWindows = 30;

  /**
   * The maximum length in bytes of a rolling file. The default value of this is
   * 1MB.
   */
  @Min(1)
  protected Long maxLength = 128 * 1024 * 1024L;

  /**
   * Maximum number of tuples per sec per partition for HDFS write.
   */
  private long maxTuplesPerSecPerPartition = 300000;

  /**
   * Minimum number of tuples per sec per partition for HDFS write.
   */
  private long minTuplesPerSecPerPartition = 30000;

  /**
   * Time interval in milliseconds to check for repartitioning
   */
  private long coolDownMillis = 1 * 60 * 1000;

  /**
   * Maximum number of S3 upload partitions
   */
  private int maxS3UploadPartitions = 16;

  /**
   * Minimum number of S3 upload partitions
   */
  private int minS3UploadPartitions = 1;

  /**
   * Maximum queue size for S3 upload
   */
  private int maxQueueSizeS3Upload = 4;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordCompactionOperator<INPUT> s3compaction = dag.addOperator("S3Compaction", new FSRecordCompactionOperator<INPUT>());
    s3compaction.setConverter(getConverter());
    s3compaction.setMaxIdleWindows(maxIdleWindows);
    s3compaction.setMaxLength(maxLength);

    StatelessThroughputBasedPartitioner<FSRecordCompactionOperator<INPUT>> partitioner = new StatelessThroughputBasedPartitioner<FSRecordCompactionOperator<INPUT>>();
    partitioner.setMaximumEvents(maxTuplesPerSecPerPartition);
    partitioner.setMinimumEvents(minTuplesPerSecPerPartition);
    partitioner.setCooldownMillis(coolDownMillis);
    dag.setAttribute(s3compaction, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[] {partitioner}));
    dag.setAttribute(s3compaction, OperatorContext.PARTITIONER, partitioner);

    S3Reconciler s3Reconciler = dag.addOperator("S3Reconciler", new S3Reconciler());
    s3Reconciler.setAccessKey(accessKey);
    s3Reconciler.setSecretKey(secretAccessKey);
    s3Reconciler.setBucketName(bucketName);
    if (region != null) {
      s3Reconciler.setRegion(region);
    }
    s3Reconciler.setDirectoryName(outputDirectoryPath);

    S3ReconcilerQueuePartitioner<S3Reconciler> reconcilerPartitioner = new S3ReconcilerQueuePartitioner<S3Reconciler>();
    reconcilerPartitioner.setCooldownMillis(coolDownMillis);
    reconcilerPartitioner.setMinPartitions(minS3UploadPartitions);
    reconcilerPartitioner.setMaxPartitions(maxS3UploadPartitions);
    reconcilerPartitioner.setMaxQueueSizePerPartition(maxQueueSizeS3Upload);

    dag.setAttribute(s3Reconciler, OperatorContext.STATS_LISTENERS,
        Arrays.asList(new StatsListener[] {reconcilerPartitioner}));
    dag.setAttribute(s3Reconciler, OperatorContext.PARTITIONER, reconcilerPartitioner);

    dag.addStream("write-to-s3", s3compaction.output, s3Reconciler.input);
    input.set(s3compaction.input);
    output.set(s3Reconciler.outputPort);
  }

  /**
   * Get the AWS access key
   *
   * @return AWS access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the AWS access key
   *
   * @param accessKey
   *          access key
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Return the AWS secret access key
   *
   * @return AWS secret access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the AWS secret access key
   *
   * @param secretAccessKey
   *          AWS secret access key
   */
  public void setSecretAccessKey(@NotNull String secretAccessKey)
  {
    this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey);
  }

  /**
   * Get the name of the bucket in which to upload the files
   *
   * @return bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the name of the bucket in which to upload the files
   *
   * @param bucketName
   *          name of the bucket
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Get the S3 Region
   * @return region
   */
  public String getRegion()
  {
    return region;
  }

  /**
   * Set the AWS S3 region
   * @param region region
   */
  public void setRegion(String region)
  {
    this.region = region;
  }

  /**
   * Get the path of the output directory.
   *
   * @return path of output directory
   */
  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  /**
   * Set the path of the output directory.
   *
   * @param outputDirectoryPath
   *          path of output directory
   */
  public void setOutputDirectoryPath(@NotNull String outputDirectoryPath)
  {
    this.outputDirectoryPath = Preconditions.checkNotNull(outputDirectoryPath);
  }

  /**
   * No. of idle window after which file should be rolled over
   *
   * @return max number of idle windows for rollover
   */
  public long getMaxIdleWindows()
  {
    return maxIdleWindows;
  }

  /**
   * No. of idle window after which file should be rolled over
   *
   * @param maxIdleWindows
   *          max number of idle windows for rollover
   */
  public void setMaxIdleWindows(long maxIdleWindows)
  {
    this.maxIdleWindows = maxIdleWindows;
  }

  /**
   * Get max length of file after which file should be rolled over
   *
   * @return max length of file
   */
  public Long getMaxLength()
  {
    return maxLength;
  }

  /**
   * Set max length of file after which file should be rolled over
   *
   * @param maxLength
   *          max length of file
   */
  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }

  public long getMaxTuplesPerSecPerPartition()
  {
    return maxTuplesPerSecPerPartition;
  }

  public void setMaxTuplesPerSecPerPartition(long maxTuplesPerSecPerPartition)
  {
    this.maxTuplesPerSecPerPartition = maxTuplesPerSecPerPartition;
  }

  public long getMinTuplesPerSecPerPartition()
  {
    return minTuplesPerSecPerPartition;
  }

  public void setMinTuplesPerSecPerPartition(long minTuplesPerSecPerPartition)
  {
    this.minTuplesPerSecPerPartition = minTuplesPerSecPerPartition;
  }

  public long getCoolDownMillis()
  {
    return coolDownMillis;
  }

  public void setCoolDownMillis(long coolDownMillis)
  {
    this.coolDownMillis = coolDownMillis;
  }

  public int getMaxS3UploadPartitions()
  {
    return maxS3UploadPartitions;
  }

  public void setMaxS3UploadPartitions(int maxS3UploadPartitions)
  {
    this.maxS3UploadPartitions = maxS3UploadPartitions;
  }

  public int getMinS3UploadPartitions()
  {
    return minS3UploadPartitions;
  }

  public void setMinS3UploadPartitions(int minS3UploadPartitions)
  {
    this.minS3UploadPartitions = minS3UploadPartitions;
  }

  public int getMaxQueueSizeS3Upload()
  {
    return maxQueueSizeS3Upload;
  }

  public void setMaxQueueSizeS3Upload(int maxQueueSizeS3Upload)
  {
    this.maxQueueSizeS3Upload = maxQueueSizeS3Upload;
  }

  /**
   * Converter for conversion of input tuples to byte[]
   *
   * @return converter
   */
  protected abstract Converter<INPUT, byte[]> getConverter();

  public static class S3BytesOutputModule extends S3TupleOutputModule<byte[]>
  {
    @Override
    protected Converter<byte[], byte[]> getConverter()
    {
      return new NoOpConverter();
    }
  }

  public static class S3StringOutputModule extends S3TupleOutputModule<String>
  {
    @Override
    protected Converter<String, byte[]> getConverter()
    {
      return new StringToBytesConverter();
    }
  }
}
