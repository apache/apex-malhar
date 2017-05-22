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
package org.apache.apex.malhar.flume.discovery;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.flume.conf.Configurable;

import com.google.common.base.Throwables;

import com.datatorrent.api.Component;

/**
 * <p>ZKAssistedDiscovery class.</p>
 *
 * @since 0.9.3
 */
public class ZKAssistedDiscovery implements Discovery<byte[]>,
    Component<com.datatorrent.api.Context>, Configurable, Serializable
{
  @NotNull
  private String serviceName;
  @NotNull
  private String connectionString;
  @NotNull
  private String basePath;
  private int connectionTimeoutMillis;
  private int connectionRetryCount;
  private int conntectionRetrySleepMillis;
  private transient InstanceSerializerFactory instanceSerializerFactory;
  private transient CuratorFramework curatorFramework;
  private transient ServiceDiscovery<byte[]> discovery;

  public ZKAssistedDiscovery()
  {
    this.serviceName = "ApexFlume";
    this.conntectionRetrySleepMillis = 500;
    this.connectionRetryCount = 10;
    this.connectionTimeoutMillis = 1000;
  }

  @Override
  public void unadvertise(Service<byte[]> service)
  {
    doAdvertise(service, false);
  }

  @Override
  public void advertise(Service<byte[]> service)
  {
    doAdvertise(service, true);
  }

  public void doAdvertise(Service<byte[]> service, boolean flag)
  {
    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      ServiceInstance<byte[]> instance = getInstance(service);
      if (flag) {
        discovery.registerService(instance);
      } else {
        discovery.unregisterService(instance);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Collection<Service<byte[]>> discover()
  {
    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      Collection<ServiceInstance<byte[]>> services = discovery.queryForInstances(serviceName);
      ArrayList<Service<byte[]>> returnable = new ArrayList<Service<byte[]>>(services.size());
      for (final ServiceInstance<byte[]> service : services) {
        returnable.add(new Service<byte[]>()
        {
          @Override
          public String getHost()
          {
            return service.getAddress();
          }

          @Override
          public int getPort()
          {
            return service.getPort();
          }

          @Override
          public byte[] getPayload()
          {
            return service.getPayload();
          }

          @Override
          public String getId()
          {
            return service.getId();
          }

          @Override
          public String toString()
          {
            return "{" + getId() + " => " + getHost() + ':' + getPort() + '}';
          }

        });
      }
      return returnable;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString()
  {
    return "ZKAssistedDiscovery{" + "serviceName=" + serviceName + ", connectionString=" + connectionString +
        ", basePath=" + basePath + ", connectionTimeoutMillis=" + connectionTimeoutMillis + ", connectionRetryCount=" +
        connectionRetryCount + ", conntectionRetrySleepMillis=" + conntectionRetrySleepMillis + '}';
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 47 * hash + this.serviceName.hashCode();
    hash = 47 * hash + this.connectionString.hashCode();
    hash = 47 * hash + this.basePath.hashCode();
    hash = 47 * hash + this.connectionTimeoutMillis;
    hash = 47 * hash + this.connectionRetryCount;
    hash = 47 * hash + this.conntectionRetrySleepMillis;
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ZKAssistedDiscovery other = (ZKAssistedDiscovery)obj;
    if (!this.serviceName.equals(other.serviceName)) {
      return false;
    }
    if (!this.connectionString.equals(other.connectionString)) {
      return false;
    }
    if (!this.basePath.equals(other.basePath)) {
      return false;
    }
    if (this.connectionTimeoutMillis != other.connectionTimeoutMillis) {
      return false;
    }
    if (this.connectionRetryCount != other.connectionRetryCount) {
      return false;
    }
    if (this.conntectionRetrySleepMillis != other.conntectionRetrySleepMillis) {
      return false;
    }
    return true;
  }

  ServiceInstance<byte[]> getInstance(Service<byte[]> service) throws Exception
  {
    return ServiceInstance.<byte[]>builder()
            .name(serviceName)
            .address(service.getHost())
            .port(service.getPort())
            .id(service.getId())
            .payload(service.getPayload())
            .build();
  }

  private ServiceDiscovery<byte[]> getDiscovery(CuratorFramework curatorFramework)
  {
    return ServiceDiscoveryBuilder.builder(byte[].class)
            .basePath(basePath)
            .client(curatorFramework)
            .serializer(instanceSerializerFactory.getInstanceSerializer(
            new TypeReference<ServiceInstance<byte[]>>()
              {})).build();
  }

  /**
   * @return the instanceSerializerFactory
   */
  InstanceSerializerFactory getInstanceSerializerFactory()
  {
    return instanceSerializerFactory;
  }

  /**
   * @return the connectionString
   */
  public String getConnectionString()
  {
    return connectionString;
  }

  /**
   * @param connectionString the connectionString to set
   */
  public void setConnectionString(String connectionString)
  {
    this.connectionString = connectionString;
  }

  /**
   * @return the basePath
   */
  public String getBasePath()
  {
    return basePath;
  }

  /**
   * @param basePath the basePath to set
   */
  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  /**
   * @return the connectionTimeoutMillis
   */
  public int getConnectionTimeoutMillis()
  {
    return connectionTimeoutMillis;
  }

  /**
   * @param connectionTimeoutMillis the connectionTimeoutMillis to set
   */
  public void setConnectionTimeoutMillis(int connectionTimeoutMillis)
  {
    this.connectionTimeoutMillis = connectionTimeoutMillis;
  }

  /**
   * @return the connectionRetryCount
   */
  public int getConnectionRetryCount()
  {
    return connectionRetryCount;
  }

  /**
   * @param connectionRetryCount the connectionRetryCount to set
   */
  public void setConnectionRetryCount(int connectionRetryCount)
  {
    this.connectionRetryCount = connectionRetryCount;
  }

  /**
   * @return the conntectionRetrySleepMillis
   */
  public int getConntectionRetrySleepMillis()
  {
    return conntectionRetrySleepMillis;
  }

  /**
   * @param conntectionRetrySleepMillis the conntectionRetrySleepMillis to set
   */
  public void setConntectionRetrySleepMillis(int conntectionRetrySleepMillis)
  {
    this.conntectionRetrySleepMillis = conntectionRetrySleepMillis;
  }

  /**
   * @return the serviceName
   */
  public String getServiceName()
  {
    return serviceName;
  }

  /**
   * @param serviceName the serviceName to set
   */
  public void setServiceName(String serviceName)
  {
    this.serviceName = serviceName;
  }

  @Override
  public void configure(org.apache.flume.Context context)
  {
    serviceName = context.getString("serviceName", "ApexFlume");
    connectionString = context.getString("connectionString");
    basePath = context.getString("basePath");

    connectionTimeoutMillis = context.getInteger("connectionTimeoutMillis", 1000);
    connectionRetryCount = context.getInteger("connectionRetryCount", 10);
    conntectionRetrySleepMillis = context.getInteger("connectionRetrySleepMillis", 500);
  }

  @Override
  public void setup(com.datatorrent.api.Context context)
  {
    ObjectMapper om = new ObjectMapper();
    instanceSerializerFactory = new InstanceSerializerFactory(om.reader(), om.writer());

    curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMillis)
            .retryPolicy(new RetryNTimes(connectionRetryCount, conntectionRetrySleepMillis))
            .connectString(connectionString)
            .build();
    curatorFramework.start();

    discovery = getDiscovery(curatorFramework);
    try {
      discovery.start();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      discovery.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      curatorFramework.close();
      curatorFramework = null;
    }
  }

  public class InstanceSerializerFactory
  {
    private final ObjectReader objectReader;
    private final ObjectWriter objectWriter;

    InstanceSerializerFactory(ObjectReader objectReader, ObjectWriter objectWriter)
    {
      this.objectReader = objectReader;
      this.objectWriter = objectWriter;
    }

    public <T> InstanceSerializer<T> getInstanceSerializer(
        TypeReference<ServiceInstance<T>> typeReference)
    {
      return new JacksonInstanceSerializer<T>(objectReader, objectWriter, typeReference);
    }

    final class JacksonInstanceSerializer<T> implements InstanceSerializer<T>
    {
      private final TypeReference<ServiceInstance<T>> typeRef;
      private final ObjectWriter objectWriter;
      private final ObjectReader objectReader;

      JacksonInstanceSerializer(ObjectReader objectReader, ObjectWriter objectWriter,
          TypeReference<ServiceInstance<T>> typeRef)
      {
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
        this.typeRef = typeRef;
      }

      @Override
      public ServiceInstance<T> deserialize(byte[] bytes) throws Exception
      {
        return objectReader.withType(typeRef).readValue(bytes);
      }

      @Override
      public byte[] serialize(ServiceInstance<T> serviceInstance) throws Exception
      {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        objectWriter.writeValue(out, serviceInstance);
        return out.toByteArray();
      }

    }

  }

  private static final long serialVersionUID = 201401221145L;
  private static final Logger logger = LoggerFactory.getLogger(ZKAssistedDiscovery.class);
}
