/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

/**
 * This is a helper class which is intended to be used for operators
 * which need to take POJOs as input and convert them into GPOMutable objects.
 */
public class GPOGetters {
  /**
   * Array of boolean getters.
   */
  public GetterBoolean<Object>[] gettersBoolean;
  /**
   * Array of char getters.
   */
  public GetterChar<Object>[] gettersChar;
  /**
   * Array of byte getters.
   */
  public GetterByte<Object>[] gettersByte;
  /**
   * Array of short getters.
   */
  public GetterShort<Object>[] gettersShort;
  /**
   * Array of int getters.
   */
  public GetterInt<Object>[] gettersInteger;
  /**
   * Array of long getters.
   */
  public GetterLong<Object>[] gettersLong;
  /**
   * Array of float getters.
   */
  public GetterFloat<Object>[] gettersFloat;
  /**
   * Array of double getters.
   */
  public GetterDouble<Object>[] gettersDouble;
  /**
   * Array of string getters.
   */
  public Getter<Object, String>[] gettersString;
  /**
   * Array of object getters.
   */
  public Getter<Object, Object>[] gettersObject;
}
