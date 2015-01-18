package com.datatorrent.lib.security;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

/**
 */
public interface SecurityContext extends Context
{

  Attribute<String> USER_NAME = new Attribute<String>((String)null);
  Attribute<char[]> PASSWORD = new Attribute<char[]>((char[])null);
  Attribute<String> REALM = new Attribute<String>((String)null);
  
}
