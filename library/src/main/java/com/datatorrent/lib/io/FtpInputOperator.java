/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTPClient;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * This operator emits each line as different tuple for a give file hosted on a ftp server <br>
 * 
 * <b>Ports</b>:<br>
 * <b>outport</b>: emits &lt;String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>filePath</b> : Path for file to be read. <br>
 * <b>delay</b>: Thread sleep interval after emitting line.<br>
 * <b>numberOfTuples</b>: Number of tuples to be emitted in a single emit Tuple call.<br>
 * <b>ftpServer</b>: The ftp server where the file is hosted.<br>
 * <b>port</b>: Port of the ftp server.<br>
 * <b>userName</b>: The user name used to login to ftp server. Default is anonymous.<br>
 * <b>password</b>: The password used to login to ftp server.<br>
 * <b>isGzip</b>: If the format of the file is gzip.<br>
 * 
 */
public class FtpInputOperator implements InputOperator
{
  /**
   * The amount of time to wait for the file to be updated.
   */
  private long delay = 10;
  /**
   * The number of lines to emit in a single window
   */
  private int numberOfTuples = 1000;
  /**
   * The ftp server to which to connect
   */
  private String ftpServer;
  /**
   * The port of the ftp server
   */
  private int port;
  /**
   * This is used for local passive mode
   */
  private boolean localPassiveMode;
  /**
   * The user name used to login to ftp server. Default is anonymous
   */
  private String userName;
  /**
   * The password used to login to ftp server.
   */
  private String password;
  /**
   * The file that needs to be read
   */
  private String filePath;
  /**
   * If the file format is gzip
   */
  private boolean isGzip;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient FTPClient ftp;
  private transient BufferedReader in;

  @Override
  public void beginWindow(long arg0)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    ftp = new FTPClient();
    if (ftpServer == null) {
      throw new RuntimeException("The ftp server can't be null");
    }
    if (filePath == null) {
      throw new RuntimeException("The file Path can't be null");
    }
    try {
      if (port != 0) {
        ftp.connect(ftpServer, port);
      } else {
        ftp.connect(ftpServer);
      }
      if (localPassiveMode) {
        ftp.enterLocalPassiveMode();
      }
      ftp.login(getUserName(), getPassword());
      InputStream is = ftp.retrieveFileStream(filePath);
      InputStreamReader reader;
      if (isGzip) {
        GZIPInputStream gzis = new GZIPInputStream(is);
        reader = new InputStreamReader(gzis);
      } else {
        reader = new InputStreamReader(is);
      }
      in = new BufferedReader(reader);
    } catch (Exception e) {
      throw new RuntimeException(e);

    }
  }

  @Override
  public void teardown()
  {
    try {
      if (ftp != null) {
        ftp.logout();
        if (ftp.isConnected()) {
          ftp.disconnect();
        }
      }
      if (in != null) {
        in.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void emitTuples()
  {
    int localCounter = numberOfTuples;
    try {
      while (localCounter > 0) {
        String str = in.readLine();
        if (str == null) {
        } else {
          output.emit(str);
        }
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        --localCounter;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the ftpServer
   */
  public String getFtpServer()
  {
    return ftpServer;
  }

  /**
   * @param ftpServer
   *          the ftpServer to set
   */
  public void setFtpServer(String ftpServer)
  {
    this.ftpServer = ftpServer;
  }

  /**
   * @return the port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the localPassiveMode
   */
  public boolean isLocalPassiveMode()
  {
    return localPassiveMode;
  }

  /**
   * @param localPassiveMode
   *          the localPassiveMode to set
   */
  public void setLocalPassiveMode(boolean localPassiveMode)
  {
    this.localPassiveMode = localPassiveMode;
  }

  /**
   * @return the userName
   */
  public String getUserName()
  {
    if (userName == null) {
      userName = "anonymous";
    }
    return userName;
  }

  /**
   * @param userName
   *          the userName to set
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * @return the password
   */
  public String getPassword()
  {
    if (password == null) {
      try {
        password = System.getProperty("user.name") + "@" + InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }
    return password;
  }

  /**
   * @param password
   *          the password to set
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * @return the delay
   */
  public long getDelay()
  {
    return delay;
  }

  /**
   * @param delay
   *          the delay to set
   */
  public void setDelay(long delay)
  {
    this.delay = delay;
  }

  /**
   * @return the numberOfTuples
   */
  public int getNumberOfTuples()
  {
    return numberOfTuples;
  }

  /**
   * @param numberOfTuples
   *          the numberOfTuples to set
   */
  public void setNumberOfTuples(int numberOfTuples)
  {
    this.numberOfTuples = numberOfTuples;
  }

  /**
   * @return the filePath
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * @param filePath
   *          the filePath to set
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * @return the isGzip
   */
  public boolean isGzip()
  {
    return isGzip;
  }

  /**
   * @param isGzip
   *          the isGzip to set
   */
  public void setGzip(boolean isGzip)
  {
    this.isGzip = isGzip;
  }

}
