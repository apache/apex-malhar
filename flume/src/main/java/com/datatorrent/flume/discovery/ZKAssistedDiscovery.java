/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.discovery;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.base.Throwables;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
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
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class ZKAssistedDiscovery implements Discovery<byte[]>, Configurable
{
  private String connectionString;
  private String basePath;
  private Integer connectionTimeoutMillis;
  private Integer connectionRetryCount;
  private Integer conntectionRetrySleepMillis;
  private InstanceSerializerFactory instanceSerializerFactory;
  private String serviceName;

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
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMillis)
            .retryPolicy(new RetryNTimes(connectionRetryCount, conntectionRetrySleepMillis))
            .connectString(connectionString)
            .build();
    curatorFramework.start();

    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      ServiceDiscovery<byte[]> discovery = getDiscovery(curatorFramework);
      discovery.start();
      ServiceInstance<byte[]> instance = getInstance(service);
      if (flag) {
        discovery.registerService(instance);
      }
      else {
        discovery.unregisterService(instance);
      }
      discovery.close();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Collection<Service<byte[]>> discover()
  {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMillis)
            .retryPolicy(new RetryNTimes(connectionRetryCount, conntectionRetrySleepMillis))
            .connectString(connectionString)
            .build();
    curatorFramework.start();

    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      ServiceDiscovery<byte[]> discovery = getDiscovery(curatorFramework);
      discovery.start();
      Collection<ServiceInstance<byte[]>> services = discovery.queryForInstances(serviceName);
      ArrayList<Service<byte[]>> returnable = new ArrayList<Service<byte[]>>(services.size());
      for (final ServiceInstance<byte[]> service: services) {
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

        });
      }
      discovery.close();
      return returnable;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void configure(Context context)
  {
    serviceName = context.getString("serviceName", "DTFlume");

    connectionString = context.getString("connectionString");
    connectionTimeoutMillis = context.getInteger("connectionTimeoutMillis", 1000);
    connectionRetryCount = context.getInteger("connectionRetryCount", 10);
    conntectionRetrySleepMillis = context.getInteger("connectionRetrySleepMillis", 500);
    basePath = context.getString("basePath");

    ObjectMapper om = new ObjectMapper();
    instanceSerializerFactory = new InstanceSerializerFactory(om.reader(), om.writer());
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
                            {
                            }
                    ))
            .build();
  }

  /**
   * @return the instanceSerializerFactory
   */
  InstanceSerializerFactory getInstanceSerializerFactory()
  {
    return instanceSerializerFactory;
  }

  public static final class SinkMetadata
  {
    private static final String BOUND_HOST = "boundHost";
    private static final String BOUND_PORT = "boundPort";

    @JsonProperty(BOUND_HOST)
    public final String boundHost;

    @JsonProperty(BOUND_PORT)
    public final int boundPort;

    @JsonCreator
    public SinkMetadata(@JsonProperty(BOUND_HOST) String boundHost, @JsonProperty(BOUND_PORT) int boundPort)
    {
      this.boundHost = boundHost;
      this.boundPort = boundPort;
    }

    /**
     * @return the boundHost
     */
    String getBoundHost()
    {
      return boundHost;
    }

    /**
     * @return the boundPort
     */
    int getBoundPort()
    {
      return boundPort;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 59 * hash + (this.boundHost != null ? this.boundHost.hashCode() : 0);
      hash = 59 * hash + this.boundPort;
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
      final SinkMetadata other = (SinkMetadata)obj;
      if ((this.boundHost == null) ? (other.boundHost != null) : !this.boundHost.equals(other.boundHost)) {
        return false;
      }
      return this.boundPort == other.boundPort;
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

      JacksonInstanceSerializer(ObjectReader objectReader, ObjectWriter objectWriter, TypeReference<ServiceInstance<T>> typeRef)
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

  private static final Logger logger = LoggerFactory.getLogger(ZKAssistedDiscovery.class);
}
