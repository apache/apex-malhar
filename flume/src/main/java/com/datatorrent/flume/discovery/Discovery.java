/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.discovery;

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
 * @param <T> - Type of the objects which can be discovered
 * @since 0.9.3
 */
public interface Discovery<T>
{
  /**
   * Recall the previously published address as it's no longer valid.
   *
   * @param service
   */
  void unadvertise(Service<T> service);

  /**
   * Advertise the host/port address where DTFlumeSink is accepting a client connection.
   *
   * @param service
   */
  void advertise(Service<T> service);

  /**
   * Discover all the addresses which are actively accepting the client connections.
   *
   * @return - Active server addresses which can accept the connections.
   */
  Collection<Service<T>> discover();

  interface Service<T>
  {
    String getHost();

    int getPort();

    T getPayload();

    String getId();

  }

}
