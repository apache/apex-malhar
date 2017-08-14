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
package org.apache.apex.malhar.lib.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

import com.datatorrent.api.DefaultOutputPort;

/**
 * An {@link AbstractFileInputOperator} that scans a remote directory via FTP for new files.<br/>
 * Files are then split in tuples which are emitted out.
 * <p/>
 * Configurations:<br/>
 * {@link #host} : ftp server host.<br/>
 * {@link #port} : ftp server port. default: {@link FTP#DEFAULT_PORT }<br/>
 * {@link #userName} : username used for login to the server. default: anonymous<br/>
 * {@link #password} : password used for login to the server. default: guest<br/>
 * <p/>
 *
 * @param <T> type of tuple.
 * @displayName FTP Directory Input
 * @category Input
 * @tags ftp
 *
 * @since 2.0.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractFTPInputOperator<T> extends AbstractFileInputOperator<T>
{
  @NotNull
  private String host;

  private int port;

  private String userName;

  private String password;

  public AbstractFTPInputOperator()
  {
    super();
    port = FTP.DEFAULT_PORT;
    userName = "anonymous";
    password = "guest";
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    FTPFileSystem ftpFileSystem = new FTPFileSystem();
    String ftpUri = "ftp://" + userName + ":" + password + "@" + host + ":" + port;
    LOG.debug("ftp uri {}", ftpUri);
    ftpFileSystem.initialize(URI.create(ftpUri), configuration);
    return ftpFileSystem;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFileInputOperator<T>>> partitions)
  {
    super.partitioned(partitions);
    for (Partition<AbstractFileInputOperator<T>> partition : partitions.values()) {
      ((AbstractFTPInputOperator<T>)partition.getPartitionedInstance()).host = host;
      ((AbstractFTPInputOperator<T>)partition.getPartitionedInstance()).port = port;
      ((AbstractFTPInputOperator<T>)partition.getPartitionedInstance()).userName = userName;
      ((AbstractFTPInputOperator<T>)partition.getPartitionedInstance()).password = password;
    }
  }

  /**
   * Sets the ftp server host.
   *
   * @param host
   */
  public void setHost(String host)
  {
    this.host = host;
  }

  /**
   * @return the ftp server host.
   */
  public String getHost()
  {
    return host;
  }

  /**
   * Sets the ftp server port
   *
   * @param port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the ftp server port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Sets the user name which is used for login to the server.
   *
   * @param userName
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * @return the user name
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * Sets the password which is used for login to the server.
   *
   * @param password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * @return the password
   */
  public String getPassword()
  {
    return password;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFTPInputOperator.class);

  /**
   * An {@link AbstractFTPInputOperator} that splits file into lines and emits them.
   *
   * @displayName FTP String Input
   * @category Input
   * @tags ftp
   */
  public static class FTPStringInputOperator extends AbstractFTPInputOperator<String>
  {
    private transient BufferedReader br;

    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br = null;
    }

    @Override
    protected String readEntity() throws IOException
    {
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
    }
  }
}
