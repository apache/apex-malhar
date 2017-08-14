/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.enrich;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.db.cache.CacheManager;
import org.apache.apex.malhar.lib.db.cache.CacheStore;
import org.apache.apex.malhar.lib.db.cache.CacheStore.ExpiryType;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Base class for Enrichment Operator.&nbsp; Subclasses should provide implementation to getKey and convert.
 * The operator receives a tuple and emits enriched tuple based on includeFields and lookupFields. <br/>
 * <p>
 * Properties:<br>
 * <b>lookupFields</b>: List of comma separated keys for quick searching. Ex: Field1,Field2,Field3<br>
 * <b>includeFields</b>: List of comma separated fields to be replaced/added to the input tuple. Ex: Field1,Field2,Field3<br>
 * <b>store</b>: Specify the type of loader for looking data<br>
 * <br>
 *
 * @param <INPUT>  Type of tuples which are received by this operator</T>
 * @param <OUTPUT> Type of tuples which are emitted by this operator</T>
 * @displayName Abstract Enrichment Operator
 * @tags Enrichment
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public abstract class AbstractEnricher<INPUT, OUTPUT> extends BaseOperator implements Operator.ActivationListener
{
  /**
   * Mandatory parameters for Enricher
   */
  @NotNull
  protected List<String> lookupFields;
  @NotNull
  protected List<String> includeFields;
  @NotNull
  private BackendLoader store;

  /**
   * Optional parameters for enricher.
   */
  private long cacheExpirationInterval = 1 * 60 * 60 * 1000;  // 1 hour
  private long cacheCleanupInterval = 1 * 60 * 60 * 1000; // 1 hour
  private ExpiryType expiryType = ExpiryType.EXPIRE_AFTER_WRITE;
  private int cacheSize = 1024; // 1024 records

  /**
   * Helper variables.
   */
  protected transient List<FieldInfo> lookupFieldInfo = new ArrayList<>();
  protected transient List<FieldInfo> includeFieldInfo = new ArrayList<>();

  private CacheManager cacheManager = new NullValuesCacheManager();

  /**
   * This method needs to be called by implementing class for processing a tuple for enrichment.
   * The method will take the tuple through following stages:
   * <ol>
   * <li>Call {@link #getKey(Object)} to retrieve key fields for lookup</li>
   * <li>Using key fields call cache manager to retrieve for any key that is cached already</li>
   * <li>If not found in cache, it'll do a lookup in configured backend store</li>
   * <li>The retrieved fields will be passed to {@link #convert(Object, Object)} method to create the final object</li>
   * <li>Finally {@link #emitEnrichedTuple(Object)} is called for emitting the tuple</li>
   * </ol>
   *
   * @param tuple Input tuple that needs to get processed for enrichment.
   */
  protected void enrichTuple(INPUT tuple)
  {
    Object key = getKey(tuple);
    if (key != null) {
      Object result = cacheManager.get(key);
      OUTPUT out = convert(tuple, result);
      if (out != null) {
        emitEnrichedTuple(out);
      }
    }
  }

  /**
   * The method should be implemented by concrete class which returns an ArrayList<Object> containing all the fields
   * which forms key part of lookup.
   * The order of field values should be same as the one set in {@link #lookupFields} variable.
   *
   * @param tuple Input tuple from which fields values for key needs to be fetched.
   * @return Should return ArrayList<Object> which has fields values forming keys in same order as {@link #lookupFields}
   */
  protected abstract Object getKey(INPUT tuple);

  /**
   * The method should be implemented by concrete class.
   * This method is expected to take input tuple and an externally fetched object containing fields to be enriched, and
   * return an enriched tuple which is ready to be emitted.
   *
   * @param in     Input tuple which needs to be enriched.
   * @param cached ArrayList<Object> containing missing data retrieved from external sources.
   * @return Enriched tuple of type OUTPUT
   */
  protected abstract OUTPUT convert(INPUT in, Object cached);

  /**
   * This method should be implemented by concrete class.
   * The method is expected to emit tuple of type OUTPUT
   *
   * @param tuple Tuple of type OUTPUT that should be emitted.
   */
  protected abstract void emitEnrichedTuple(OUTPUT tuple);

  /**
   * This method should be implemented by concrete method.
   * The method should return Class type of field for given fieldName from output tuple.
   *
   * @param fieldName Field name for which field type needs to be identified
   * @return Class type for given field.
   */
  protected abstract Class<?> getIncludeFieldType(String fieldName);

  /**
   * This method should be implemented by concrete method.
   * The method should return Class type of field for given fieldName from input tuple.
   *
   * @param fieldName Field name for which field type needs to be identified
   * @return Class type for given field.
   */
  protected abstract Class<?> getLookupFieldType(String fieldName);

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    CacheStore primaryCache = new CacheStore();

    // set expiration to one day.
    primaryCache.setEntryExpiryDurationInMillis(cacheExpirationInterval);
    primaryCache.setCacheCleanupInMillis(cacheCleanupInterval);
    primaryCache.setEntryExpiryStrategy(expiryType);
    primaryCache.setMaxCacheSize(cacheSize);

    cacheManager.setPrimary(primaryCache);
    cacheManager.setBackup(store);
  }

  @Override
  public void activate(Context context)
  {
    for (String s : lookupFields) {
      lookupFieldInfo.add(new FieldInfo(s, s, SupportType.getFromJavaType(getLookupFieldType(s))));
    }

    if (includeFields != null) {
      for (String s : includeFields) {
        includeFieldInfo.add(new FieldInfo(s, s, SupportType.getFromJavaType(getIncludeFieldType(s))));
      }
    }

    store.setFieldInfo(lookupFieldInfo, includeFieldInfo);

    try {
      cacheManager.initialize();
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize cache manager", e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      cacheManager.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to close cache manager", e);
    }
  }

  /**
   * Returns a list of fields which are used for lookup.
   *
   * @return List of fields
   */
  public List<String> getLookupFields()
  {
    return lookupFields;
  }

  /**
   * Set fields on which lookup needs to happen in external store.
   * This is a mandatory parameter.
   *
   * @param lookupFields List of fields on which lookup happens.
   */
  public void setLookupFields(List<String> lookupFields)
  {
    this.lookupFields = lookupFields;
  }

  /**
   * Returns a list of fields using which tuple is enriched
   *
   * @return List of fields.
   */
  public List<String> getIncludeFields()
  {
    return includeFields;
  }

  /**
   * Sets list of fields to be fetched from external store for enriching the tuple.
   * This is a mandatory parameter.
   *
   * @param includeFields List of fields.
   */
  public void setIncludeFields(List<String> includeFields)
  {
    this.includeFields = includeFields;
  }

  /**
   * Returns the backend store which will enrich the tuple.
   *
   * @return Object of type {@link BackendLoader}
   */
  public BackendLoader getStore()
  {
    return store;
  }

  /**
   * Sets backend store which will enrich the tuple.
   * This is a mandatory parameter.
   *
   * @param store Object of type {@link BackendLoader}
   */
  public void setStore(BackendLoader store)
  {
    this.store = store;
  }

  /**
   * Returns cache entry expiration interval in ms.
   * This is an optional parameter.
   *
   * @return Cache entry expiration interval in ms
   */
  public long getCacheExpirationInterval()
  {
    return cacheExpirationInterval;
  }

  /**
   * Sets cache entry expiration interval in ms.
   * This is an optional parameter.
   *
   * @param cacheExpirationInterval Cache entry expiration interval in ms
   */
  public void setCacheExpirationInterval(long cacheExpirationInterval)
  {
    this.cacheExpirationInterval = cacheExpirationInterval;
  }

  /**
   * Returns cache cleanup interval in ms. After this interval, cache cleanup operation will be performed.
   * This is an optional parameter.
   *
   * @return cache cleanup interval in ms.
   */
  public long getCacheCleanupInterval()
  {
    return cacheCleanupInterval;
  }

  /**
   * Set Cache cleanup interval in ms. After this interval, cache cleanup operation will be performed.
   * This is an optional parameter.
   *
   * @param cacheCleanupInterval cache cleanup interval in ms.
   */
  public void setCacheCleanupInterval(long cacheCleanupInterval)
  {
    this.cacheCleanupInterval = cacheCleanupInterval;
  }

  /**
   * Get size (number of entries) of cache.
   *
   * @return Number of entries allowed in cache.
   */
  public int getCacheSize()
  {
    return cacheSize;
  }

  /**
   * Set size (number of entries) of cache.
   *
   * @param cacheSize Number of entries allowed in cache.
   */
  public void setCacheSize(int cacheSize)
  {
    this.cacheSize = cacheSize;
  }

  public ExpiryType getExpiryType()
  {
    return expiryType;
  }

  public void setExpiryType(ExpiryType expiryType)
  {
    this.expiryType = expiryType;
  }

  public CacheManager getCacheManager()
  {
    return cacheManager;
  }

  public void setCacheManager(CacheManager cacheManager)
  {
    this.cacheManager = cacheManager;
  }
}
