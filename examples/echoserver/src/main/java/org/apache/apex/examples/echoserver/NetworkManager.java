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
package org.apache.apex.examples.echoserver;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.1.0
 */
public class NetworkManager implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(NetworkManager.class);

  public static enum ConnectionType
  {
    TCP,
    UDP
  }

  private static NetworkManager _instance;
  private Selector selector;

  private volatile boolean doRun = false;
  private Thread selThread;
  private long selTimeout = 1000;
  private volatile Exception selEx;

  private Map<ConnectionInfo, ChannelConfiguration> channels;
  private Map<SelectableChannel, ChannelConfiguration> channelConfigurations;

  public static NetworkManager getInstance() throws IOException
  {
    if (_instance == null) {
      synchronized (NetworkManager.class) {
        if (_instance == null) {
          _instance = new NetworkManager();
        }
      }
    }
    return _instance;
  }

  private NetworkManager() throws IOException
  {
    channels = new HashMap<ConnectionInfo, ChannelConfiguration>();
    channelConfigurations = new HashMap<SelectableChannel, ChannelConfiguration>();
  }

  public synchronized <T extends SelectableChannel> ChannelAction<T> registerAction(int port, ConnectionType type, ChannelListener<T> listener, int ops) throws IOException
  {
    boolean startProc = (channels.size() == 0);
    SelectableChannel channel = null;
    SocketAddress address = new InetSocketAddress(port);
    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.address =  address;
    connectionInfo.connectionType = type;
    ChannelConfiguration channelConfiguration = channels.get(connectionInfo);
    if (channelConfiguration == null) {
      Object socket = null;
      if (type == ConnectionType.TCP) {
        SocketChannel schannel = SocketChannel.open();
        schannel.configureBlocking(false);
        Socket ssocket = schannel.socket();
        ssocket.bind(address);
        socket = ssocket;
        channel = schannel;
      } else if (type == ConnectionType.UDP) {
        DatagramChannel dchannel = DatagramChannel.open();
        dchannel.configureBlocking(false);
        DatagramSocket dsocket = dchannel.socket();
        dsocket.bind(address);
        socket = dsocket;
        channel = dchannel;
      }
      if (channel == null) {
        throw new IOException("Unsupported connection type");
      }
      channelConfiguration = new ChannelConfiguration();
      channelConfiguration.actions = new ConcurrentLinkedQueue<ChannelAction>();
      channelConfiguration.channel = channel;
      channelConfiguration.connectionInfo = connectionInfo;
      channels.put(connectionInfo, channelConfiguration);
      channelConfigurations.put(channel, channelConfiguration);
    } else {
      channel = channelConfiguration.channel;
    }
    ChannelAction channelAction = new ChannelAction();
    channelAction.channelConfiguration = channelConfiguration;
    channelAction.listener = listener;
    channelAction.ops = ops;
    channelConfiguration.actions.add(channelAction);
    if (startProc) {
      startProcess();
    }
    if (listener != null) {
      channel.register(selector, ops);
    }
    return channelAction;
  }

  public synchronized void unregisterAction(ChannelAction action) throws IOException, InterruptedException
  {
    ChannelConfiguration channelConfiguration = action.channelConfiguration;
    SelectableChannel channel = channelConfiguration.channel;
    if (channelConfiguration != null) {
      channelConfiguration.actions.remove(action);
      if (channelConfiguration.actions.size() == 0) {
        ConnectionInfo connectionInfo = channelConfiguration.connectionInfo;
        channelConfigurations.remove(channel);
        channels.remove(connectionInfo);
        channel.close();
      }
    }
    if (channels.size() == 0) {
      stopProcess();
    }
  }

  private void startProcess() throws IOException
  {
    selector = Selector.open();
    doRun = true;
    selThread = new Thread(this);
    selThread.start();
  }

  private void stopProcess() throws InterruptedException, IOException
  {
    doRun = false;
    selThread.join();
    selector.close();
  }

  @Override
  public void run()
  {
    try {
      while (doRun) {
        int keys = selector.select(selTimeout);
        if (keys > 0) {
          Set<SelectionKey> selectionKeys = selector.selectedKeys();
          for (SelectionKey selectionKey : selectionKeys) {
            int readyOps = selectionKey.readyOps();
            ChannelConfiguration channelConfiguration = channelConfigurations.get(selectionKey.channel());
            Collection<ChannelAction> actions = channelConfiguration.actions;
            for (ChannelAction action : actions) {
              if (((readyOps & action.ops) != 0) && (action.listener != null)) {
                action.listener.ready(action, readyOps);
              }
            }
          }
          selectionKeys.clear();
        }
      }
    } catch (IOException e) {
      logger.error("Error in select", e);
      selEx = e;
    }
  }

  public static interface ChannelListener<T extends SelectableChannel>
  {
    public void ready(ChannelAction<T> action, int readyOps);
  }

  public static class ChannelConfiguration<T extends SelectableChannel>
  {
    public T channel;
    public ConnectionInfo connectionInfo;
    public Collection<ChannelAction> actions;
  }

  public static class ChannelAction<T extends SelectableChannel>
  {
    public ChannelConfiguration<T> channelConfiguration;
    public ChannelListener<T> listener;
    public int ops;
  }

  private static class ConnectionInfo
  {
    public SocketAddress address;
    public ConnectionType connectionType;

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ConnectionInfo that = (ConnectionInfo)o;

      if (connectionType != that.connectionType) {
        return false;
      }
      if (!address.equals(that.address)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = address.hashCode();
      result = 31 * result + connectionType.hashCode();
      return result;
    }
  }

}
