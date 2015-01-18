package com.datatorrent.lib.security.auth.callback;

import java.io.IOException;

import javax.security.auth.callback.*;
import javax.security.sasl.RealmCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.security.SecurityContext;

import com.datatorrent.api.Component;

/**
*/
public class DefaultCallbackHandler implements CallbackHandler, Component<SecurityContext>
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultCallbackHandler.class);

  protected SecurityContext context;

  @Override
  public void setup(SecurityContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
  {
    for (Callback callback : callbacks) {
      processCallback(callback);
    }
  }

  protected void processCallback(Callback callback) throws IOException, UnsupportedCallbackException
  {
    if (callback instanceof NameCallback) {
      NameCallback namecb = (NameCallback)callback;
      namecb.setName(context.getValue(SecurityContext.USER_NAME));
    } else if (callback instanceof PasswordCallback) {
      PasswordCallback passcb = (PasswordCallback)callback;
      passcb.setPassword(context.getValue(SecurityContext.PASSWORD));
    } else if (callback instanceof RealmCallback) {
      RealmCallback realmcb = (RealmCallback) callback;
      realmcb.setText(context.getValue(SecurityContext.REALM));
    } else if (callback instanceof TextOutputCallback) {
      TextOutputCallback textcb = (TextOutputCallback)callback;
      if (textcb.getMessageType() == TextOutputCallback.INFORMATION) {
        logger.info(textcb.getMessage());
      } else if (textcb.getMessageType() == TextOutputCallback.WARNING) {
        logger.warn(textcb.getMessage());
      } else if (textcb.getMessageType() == TextOutputCallback.ERROR) {
        logger.error(textcb.getMessage());
      } else {
        logger.debug("Auth message type {}, message {}", textcb.getMessageType(), textcb.getMessage());
      }
    } else {
      throw new UnsupportedCallbackException(callback);
    }
  }
}
