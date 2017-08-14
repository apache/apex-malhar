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
package org.apache.apex.malhar.lib.io.jms;

import java.io.IOException;

import javax.jms.JMSException;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.annotation.Stateless;

/**
 * This is a JMS store which stores committed window ids in a file. This is not a true
 * transactionable store because there is a chance that a failure may occur in between storing the
 * committed window and committing the JMS transaction. A failure in such a scenario could cause JMS Messages
 * to be missed or duplicated. However, the chance of this is very small. If you need 100% reliability use
 * the {@link JMSTransactionableStore}.
 *
 * @since 2.0.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSPsuedoTransactionableStore extends JMSBaseTransactionableStore
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(FSPsuedoTransactionableStore.class);

  /**
   * The default directory in which recovery occurs.
   */
  public static final String DEFAULT_RECOVERY_DIRECTORY = "recovery";
  /**
   * The name of the committed window file.
   */
  public static final String COMMITTED_WINDOW_DIR = "DT_CMT";

  /**
   * Indicates whether the store is connected or not.
   */
  private transient boolean connected = false;
  /**
   * Indicates whether the store is in a transaction or not.
   */
  private transient boolean inTransaction = false;

  /**
   * The path of the directory to where files are written.
   */
  @NotNull
  protected String recoveryDirectory = DEFAULT_RECOVERY_DIRECTORY;

  /**
   * File system for storing committed window Ids.
   */
  private transient FileSystem fs;

  public FSPsuedoTransactionableStore()
  {
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   * This method is mainly helpful for unit testing.
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(recoveryDirectory).toUri(), new Configuration());

    if (tempFS instanceof LocalFileSystem) {
      tempFS = ((LocalFileSystem)tempFS).getRaw();
    }

    return tempFS;
  }

  /**
   * Sets the directory path in which committed windows are stored.
   * @param recoveryDirectory The directory path in which committed windows are stored.
   */
  public void setRecoveryDirectory(String recoveryDirectory)
  {
    this.recoveryDirectory = recoveryDirectory;
  }

  /**
   * Gets the directory path in which committed windows are stored.
   * @return The directory path in which committed windows are stored.
   */
  public String getRecoveryDirectory()
  {
    return recoveryDirectory;
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    Path recoveryPath = getOperatorRecoveryPath(appId, operatorId);

    try {
      //No committed window stored, return negative invalid window.
      if (!fs.exists(recoveryPath)) {
        return Stateless.WINDOW_ID;
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    long maxWindow = Long.MIN_VALUE;

    try {
      FileStatus[] windowFiles = fs.listStatus(recoveryPath);

      for (FileStatus fileStatus : windowFiles) {
        String windowString = fileStatus.getPath().getName();
        long tempWindow = Long.parseLong(windowString);

        if (maxWindow < tempWindow) {
          maxWindow = tempWindow;
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return maxWindow;
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    Path recoveryPath = getOperatorRecoveryPath(appId, operatorId);
    Path windowPath = getOperatorWindowRecoveryPath(appId, operatorId, windowId);
    String windowString = Long.toString(windowId);

    try {
      fs.create(windowPath);
      FileStatus[] windowFiles = fs.listStatus(recoveryPath);

      for (FileStatus fileStatus : windowFiles) {
        Path tempPath = fileStatus.getPath();
        if (!tempPath.getName().equals(windowString)) {
          fs.delete(tempPath, true);
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    try {
      fs.delete(getOperatorRecoveryPath(appId, operatorId).getParent(), true);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginTransaction()
  {
    inTransaction = true;
  }

  @Override
  public void commitTransaction()
  {
    try {
      this.getBase().getSession().commit();
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    inTransaction = false;
  }

  @Override
  public void rollbackTransaction()
  {
    try {
      this.getBase().getSession().rollback();
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    inTransaction = false;
  }

  @Override
  public boolean isInTransaction()
  {
    return inTransaction;
  }

  @Override
  public void connect() throws IOException
  {
    fs = getFSInstance();

    connected = true;
  }

  @Override
  public void disconnect() throws IOException
  {
    fs.close();

    connected = false;
  }

  @Override
  public boolean isConnected()
  {
    return connected;
  }

  @Override
  protected boolean isExactlyOnce()
  {
    return false;
  }

  private Path getOperatorRecoveryPath(String appId, int operatorId)
  {
    return new Path(DEFAULT_RECOVERY_DIRECTORY + "/" + appId + "/" + operatorId + "/" + COMMITTED_WINDOW_DIR);
  }

  /**
   * Helper method to get the path where the windowId is stored.
   * @param appId The id of the application.
   * @param operatorId The operator id of the application.
   * @param windowId The id of the current window.
   * @return The path where the windowId is stored.
   */
  private Path getOperatorWindowRecoveryPath(String appId, int operatorId, long windowId)
  {
    return new Path(DEFAULT_RECOVERY_DIRECTORY + "/" + appId + "/" + operatorId + "/" + COMMITTED_WINDOW_DIR + "/" +
        windowId);
  }
}
