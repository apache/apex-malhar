/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.converter.Converter;


public class AppDataTabularServerConv<INPUT_EVENT> extends AppDataTabularServer<INPUT_EVENT>
{
  protected Converter<INPUT_EVENT, GPOMutable, SchemaTabular> converter;

  public AppDataTabularServerConv()
  {
  }

  public void setConverter(Converter<INPUT_EVENT, GPOMutable, SchemaTabular> converter)
  {
    this.converter = converter;
  }

  public Converter<INPUT_EVENT, GPOMutable, SchemaTabular> getConverter()
  {
    return converter;
  }

  @Override
  public GPOMutable convert(INPUT_EVENT inputEvent)
  {
    return converter.convert(inputEvent, schema);
  }
}
