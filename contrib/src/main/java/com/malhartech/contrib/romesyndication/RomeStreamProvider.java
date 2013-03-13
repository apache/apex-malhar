/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public interface RomeStreamProvider
{
    public InputStream getInputStream() throws IOException;
}
