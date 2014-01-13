/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import java.io.IOException;

/**
 *
 * @since 0.9.3
 */
public interface Connectable
{
  public void connect() throws IOException;
  public void disconnect() throws IOException;
  public boolean isConnected();
}
