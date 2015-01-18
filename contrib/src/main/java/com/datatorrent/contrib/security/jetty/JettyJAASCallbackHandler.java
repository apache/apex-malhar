package com.datatorrent.contrib.security.jetty;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.eclipse.jetty.plus.jaas.callback.ObjectCallback;

import com.datatorrent.lib.security.SecurityContext;
import com.datatorrent.lib.security.auth.callback.DefaultCallbackHandler;

/**
 * A callback handler to use with Jetty login module for gateway authentication
 */
public class JettyJAASCallbackHandler extends DefaultCallbackHandler
{
  @Override
  protected void processCallback(Callback callback) throws IOException, UnsupportedCallbackException
  {
    if (callback instanceof ObjectCallback) {
      ObjectCallback objcb = (ObjectCallback) callback;
      objcb.setObject(context.getValue(SecurityContext.PASSWORD));
    } else {
      super.processCallback(callback);
    }
  }
}
