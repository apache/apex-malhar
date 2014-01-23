/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.codehaus.jackson.annotate.JsonProperty;

import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * <p>
 * To insert an object in Couch Db, any concrete implementation of {@link AbstractCouchDBOutputOperator}  needs to
 * provide additional information about the tuple that can be saved in the database. This interface outlines the api
 * of the event.
 * </p>
 * <p>
 * The command would either update or insert a document in the db. If the docId is already present then the existing
 * document will be updated or otherwise it will be created.</br>
 * </p>
 * <p>
 * When a document already exists in the db then couch-db expects the tuple to have a revision greater than the revision
 * in the database.
 * </p>
 *
 * @since 0.9.1
 */
public interface CouchDbEvent
{

  @Nullable
  String getId();

  /**
   * @return object that is compatible with {@link org.ektorp.CouchDbConnector}
   */
  @Nonnull
  Object getPayLoad();
}
