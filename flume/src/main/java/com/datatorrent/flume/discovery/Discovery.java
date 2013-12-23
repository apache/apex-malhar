/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.discovery;

import java.net.SocketAddress;
import java.util.Collection;

/**
 * When DTFlumeSink server instance binds to the network interface, it can publish
 * its whereabouts by invoking advertise method on the Discovery object. Similarly
 * when it ceases accepting any more connections, it can publish its intent to do
 * so by invoking unadvertise.<p />
 * Interesting parties can call discover method to get the list of addresses where
 * they can find an available DTFlumeSink server instance.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface Discovery
{

  /**
   * Advertise the host/port address where DTFlumeSink is accepting a client connection.
   *
   * @param serverAddress - the address which is awaiting client connection.
   */
  void unadvertise(SocketAddress serverAddress);

  /**
   * Recall the previously published address as it's no longer valid.
   *
   * @param serverAddress - previously published address which can no longer accept client connections.
   */
  public void advertise(SocketAddress serverAddress);

  /**
   * Discover all the addresses which are actively accepting the client connections.
   *
   * @return - Active server addresses which can accept the connections.
   */
  Collection<SocketAddress> discover();

}
