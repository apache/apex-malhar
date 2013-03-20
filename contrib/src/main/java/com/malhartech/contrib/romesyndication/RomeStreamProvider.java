/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import java.io.IOException;
import java.io.InputStream;

/**
 * The interface to implement for any provider that wants to provide syndication feed.<p><br>
 *
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public interface RomeStreamProvider
{
  /**
   * Get the feed input stream.
   *
   * @return The feed input stream
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException;

}
