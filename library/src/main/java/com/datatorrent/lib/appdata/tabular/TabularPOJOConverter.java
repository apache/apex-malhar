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

import com.datatorrent.lib.appbuilder.convert.pojo.PojoFieldRetriever;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

import java.util.List;

public class TabularPOJOConverter implements Converter<Object, GPOMutable, SchemaTabular>
{
  @NotNull
  private PojoFieldRetriever pojoFieldRetriever;

  public TabularPOJOConverter()
  {
  }

  @Override
  public GPOMutable convert(Object inputEvent, SchemaTabular context)
  {
    FieldsDescriptor fd = context.getValuesDescriptor();
    GPOMutable values = new GPOMutable(fd);

    List<String> fields = fd.getFieldList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);
      values.setField(field, pojoFieldRetriever.get(field, inputEvent));
    }

    return values;
  }

  /**
   * @return the pojoFieldRetriever
   */
  public PojoFieldRetriever getPojoFieldRetriever()
  {
    return pojoFieldRetriever;
  }

  /**
   * @param pojoFieldRetriever the pojoFieldRetriever to set
   */
  public void setPojoFieldRetriever(@NotNull PojoFieldRetriever pojoFieldRetriever)
  {
    this.pojoFieldRetriever = Preconditions.checkNotNull(pojoFieldRetriever);
  }
}
