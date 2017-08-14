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

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 *  A concrete implementation of AbstractJdbcTransactionableOutputOperator for Redshift which takes FSRecordCompactionOperator.OutputMetaData.
 *  Load the data into the specified redshift tables from data files. The files can be located either in S3 or an Amazon EMR.
 *  Specify the bucketName property if the file is located in S3 or specify the emrClusterId if the file is location in EMR.
 *  By default, it load files from S3 into Redshfit table. If the file is located in EMR, then specify "readFromS3" parameter to false.
 *
 * @displayName Redshift Output Operator
 * @category Output
 * @tags database, jdbc, redshift
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
@OperatorAnnotation(partitionable = false)
public class RedshiftJdbcTransactionableOutputOperator extends AbstractJdbcTransactionableOutputOperator<FSRecordCompactionOperator.OutputMetaData>
{
  private static final Logger logger = LoggerFactory.getLogger(RedshiftJdbcTransactionableOutputOperator.class);
  protected static final String DEFAULT_REDSHIFT_DELIMITER = "|";
  @NotNull
  private String tableName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String redshiftDelimiter = DEFAULT_REDSHIFT_DELIMITER;
  private String region;
  @NotNull
  private RedshiftOutputModule.READER_MODE readerMode;
  private String emrClusterId;
  private String bucketName;
  protected transient Statement stmt;

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (readerMode == RedshiftOutputModule.READER_MODE.READ_FROM_S3) {
      Preconditions.checkNotNull(bucketName);
    } else {
      Preconditions.checkNotNull(emrClusterId);
    }
    super.setup(context);
    try {
      stmt = store.getConnection().createStatement();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String getUpdateCommand()
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  // Preparedstatement is not needed for uploading data into redshift. So, nothing to be done in activate state.
  @Override
  public void activate(Context.OperatorContext context)
  {
  }

  /**
   * Create the copy statement from the specified OutputMetaData
   * @param data Given OutputMetaData
   * @return the copy statement
   */

  protected String generateCopyStatement(FSRecordCompactionOperator.OutputMetaData data)
  {
    String file = data.getPath();
    StringBuilder exec = new StringBuilder();
    exec.append("COPY " + tableName + " ");
    if (readerMode == RedshiftOutputModule.READER_MODE.READ_FROM_S3) {
      exec.append("FROM 's3://" + bucketName + "/" + file + "' ");
    } else {
      exec.append("FROM 'emr://" + emrClusterId + "/" + file + "' ");
    }
    exec.append("CREDENTIALS 'aws_access_key_id=" + accessKey);
    exec.append(";aws_secret_access_key=" + secretKey + "' ");
    if (region != null) {
      exec.append("region '" + region + "' ");
    }
    exec.append("DELIMITER '" + redshiftDelimiter + "'");
    exec.append(";");
    return exec.toString();
  }

  @Override
  protected void processBatch()
  {
    logger.debug("start {} end {}", batchStartIdx, tuples.size());
    try {
      for (int i = batchStartIdx; i < tuples.size(); i++) {
        String copyStmt = generateCopyStatement(tuples.get(i));
        stmt.addBatch(copyStmt);
      }
      stmt.executeBatch();
      stmt.clearBatch();
      batchStartIdx += tuples.size() - batchStartIdx;
    } catch (BatchUpdateException bue) {
      logger.error(bue.getMessage());
      processUpdateCounts(bue.getUpdateCounts(), tuples.size() - batchStartIdx);
    } catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, FSRecordCompactionOperator.OutputMetaData tuple) throws SQLException
  {
    throw new UnsupportedOperationException("Unsupported Operation");
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
   * @param tableName table name
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
   * @param accessKey given accessKey
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
   * @param secretKey secretkey
   */
  public void setSecretKey(@NotNull String secretKey)
  {
    this.secretKey = Preconditions.checkNotNull(secretKey);
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
   * @param redshiftDelimiter given redshiftDelimiter
   */
  public void setRedshiftDelimiter(@NotNull String redshiftDelimiter)
  {
    this.redshiftDelimiter = Preconditions.checkNotNull(redshiftDelimiter);
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
   * @param region region
   */
  public void setRegion(String region)
  {
    this.region = region;
  }

  /**
   * Specifies whether the input files read from S3 or emr
   * @return mode
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
   * Return the emrClusterId if the input files are located in EMR.
   * @return emrClusterId
   */
  public String getEmrClusterId()
  {
    return emrClusterId;
  }

  /**
   * Set the emrClusterId if the input files are located in EMR.
   * @param emrClusterId emrClusterId
   */
  public void setEmrClusterId(String emrClusterId)
  {
    this.emrClusterId = emrClusterId;
  }

  /**
   * Get the bucket name only if the input files are located in S3.
   * @return bucketName.
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name only if the input files are located in S3.
   * @param bucketName bucketName
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
