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
package org.apache.apex.malhar.lib.db.redshift;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.s3.S3TupleOutputModule;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

import static org.apache.apex.malhar.lib.db.redshift.RedshiftJdbcTransactionableOutputOperator.DEFAULT_REDSHIFT_DELIMITER;
import static org.apache.apex.malhar.lib.db.redshift.RedshiftOutputModule.READER_MODE.READ_FROM_S3;

/**
 * Functionality of RedshiftOutputModule is load data into Redshift table. Data intermediately writes to HDFS/S3 and
 * rolling files will load into Redshift table using copy command.
 * By default, it load files from S3 into Redshfit table. If the file is located in EMR, then specify "readFromS3" parameter to false.
 *
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class RedshiftOutputModule implements Module
{
  @NotNull
  private String tableName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  private String region;
  private String bucketName;
  private String directoryName;
  private String emrClusterId;
  @NotNull
  private String redshiftDelimiter = DEFAULT_REDSHIFT_DELIMITER;
  protected static enum READER_MODE
  {
    READ_FROM_S3, READ_FROM_EMR;
  }

  private READER_MODE readerMode = READ_FROM_S3;
  private int batchSize = 100;
  private Long maxLengthOfRollingFile;
  private JdbcTransactionalStore store = new JdbcTransactionalStore();

  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    if (readerMode == READ_FROM_S3) {
      S3TupleOutputModule.S3BytesOutputModule tupleBasedS3 = dag.addModule("S3Compaction", new S3TupleOutputModule.S3BytesOutputModule());
      tupleBasedS3.setAccessKey(accessKey);
      tupleBasedS3.setSecretAccessKey(secretKey);
      tupleBasedS3.setBucketName(bucketName);
      tupleBasedS3.setOutputDirectoryPath(directoryName);
      if (maxLengthOfRollingFile != null) {
        tupleBasedS3.setMaxLength(maxLengthOfRollingFile);
      }
      input.set(tupleBasedS3.input);

      RedshiftJdbcTransactionableOutputOperator redshiftOutput = dag.addOperator("LoadToRedshift", createRedshiftOperator());

      dag.addStream("load-to-redshift", tupleBasedS3.output, redshiftOutput.input);
    } else {
      FSRecordCompactionOperator<byte[]> hdfsWriteOperator = dag.addOperator("WriteToHDFS", new FSRecordCompactionOperator<byte[]>());
      hdfsWriteOperator.setConverter(new GenericFileOutputOperator.NoOpConverter());
      if (maxLengthOfRollingFile != null) {
        hdfsWriteOperator.setMaxLength(maxLengthOfRollingFile);
      }
      input.set(hdfsWriteOperator.input);

      RedshiftJdbcTransactionableOutputOperator redshiftOutput = dag.addOperator("LoadToRedshift", createRedshiftOperator());

      dag.addStream("load-to-redshift", hdfsWriteOperator.output, redshiftOutput.input);
    }
  }

  /**
   * Create the RedshiftJdbcTransactionableOutputOperator instance
   * @return RedshiftJdbcTransactionableOutputOperator object
   */
  protected RedshiftJdbcTransactionableOutputOperator createRedshiftOperator()
  {
    RedshiftJdbcTransactionableOutputOperator redshiftOutput = new RedshiftJdbcTransactionableOutputOperator();
    redshiftOutput.setAccessKey(accessKey);
    redshiftOutput.setSecretKey(secretKey);
    if (bucketName != null) {
      redshiftOutput.setBucketName(bucketName);
    }
    redshiftOutput.setTableName(tableName);
    if (emrClusterId != null) {
      redshiftOutput.setEmrClusterId(emrClusterId);
    }
    redshiftOutput.setReaderMode(readerMode.toString());
    redshiftOutput.setStore(store);
    redshiftOutput.setBatchSize(batchSize);
    redshiftOutput.setRedshiftDelimiter(redshiftDelimiter);
    if (region != null) {
      redshiftOutput.setRegion(region);
    }
    return redshiftOutput;
  }

  /**
   * Get the table name from database
   * @return tableName
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Set the name of the table as it stored in redshift
   * @param tableName given tableName
   */
  public void setTableName(@NotNull String tableName)
  {
    this.tableName = Preconditions.checkNotNull(tableName);
  }

  /**
   * Get the AWS Access key
   * @return accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the AWS Access Key
   * @param accessKey accessKey
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Get the AWS secret key
   * @return secretKey
   */
  public String getSecretKey()
  {
    return secretKey;
  }

  /**
   * Set the AWS secret key
   * @param secretKey secretKey
   */
  public void setSecretKey(@NotNull String secretKey)
  {
    this.secretKey = Preconditions.checkNotNull(secretKey);
  }

  /**
   * Get the AWS region from where the input file resides.
   * @return region
   */
  public String getRegion()
  {
    return region;
  }

  /**
   * Set the AWS region from where the input file resides.
   * This is mandatory property if S3/EMR and Redshift runs in different regions.
   * @param region given region
   */
  public void setRegion(String region)
  {
    this.region = region;
  }

  /**
   * Get the bucket name only if the input files are located in S3.
   * @return bucketName
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name only if the input files are located in S3.
   * @param bucketName bucketName
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Return the directory name under S3 bucket
   * @return directoryName
   */
  public String getDirectoryName()
  {
    return directoryName;
  }

  /**
   * Set the directory name under S3 bucket.
   * @param directoryName directoryName
   */
  public void setDirectoryName(@NotNull String directoryName)
  {
    this.directoryName = Preconditions.checkNotNull(directoryName);
  }

  /**
   * Get the EMR cluster id
   * @return emrClusterId
   */
  public String getEmrClusterId()
  {
    return emrClusterId;
  }

  /**
   * Set the EMR cluster id
   * @param emrClusterId emrClusterId
   */
  public void setEmrClusterId(@NotNull String emrClusterId)
  {
    this.emrClusterId = Preconditions.checkNotNull(emrClusterId);
  }

  /**
   * Return the delimiter character which is used to separate fields from input file.
   * @return redshiftDelimiter
   */
  public String getRedshiftDelimiter()
  {
    return redshiftDelimiter;
  }

  /**
   * Set the delimiter character which is used to separate fields from input file.
   * @param redshiftDelimiter redshiftDelimiter
   */
  public void setRedshiftDelimiter(@NotNull String redshiftDelimiter)
  {
    this.redshiftDelimiter = Preconditions.checkNotNull(redshiftDelimiter);
  }

  /**
   * Specifies whether the input files read from S3 or emr
   * @return readerMode
   */
  public String getReaderMode()
  {
    return readerMode.toString();
  }

  /**
   * Set the readFromS3 which indicates whether the input files read from S3 or emr
   * @param readerMode Type of reader mode
   */
  public void setReaderMode(@Pattern(regexp = "READ_FROM_S3|READ_FROM_EMR", flags = Pattern.Flag.CASE_INSENSITIVE) String readerMode)
  {
    this.readerMode = RedshiftOutputModule.READER_MODE.valueOf(readerMode);
  }

  /**
   * Get the size of a batch operation.
   * @return batchSize
   */
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Sets the size of a batch operation.
   * @param batchSize batchSize
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Get the maximum length in bytes of a rolling file.
   * @return maxLengthOfRollingFile
   */
  public Long getMaxLengthOfRollingFile()
  {
    return maxLengthOfRollingFile;
  }

  /**
   * Set the maximum length in bytes of a rolling file.
   * @param maxLengthOfRollingFile maxLengthOfRollingFile
   */
  public void setMaxLengthOfRollingFile(Long maxLengthOfRollingFile)
  {
    this.maxLengthOfRollingFile = maxLengthOfRollingFile;
  }

  /**
   * Get the JdbcTransactionalStore of a RedshiftJdbcTransactionableOutputOperator
   * @return JdbcTransactionalStore
   */
  public JdbcTransactionalStore getStore()
  {
    return store;
  }

  /**
   * Set the JdbcTransactionalStore
   * @param store store
   */
  public void setStore(@NotNull JdbcTransactionalStore store)
  {
    this.store = Preconditions.checkNotNull(store);
  }
}
