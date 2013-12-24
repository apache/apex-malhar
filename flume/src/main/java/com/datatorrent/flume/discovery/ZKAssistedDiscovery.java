/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.discovery;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
public class ZKAssistedDiscovery implements Discovery, Configurable
{
  private String connectionString;
  private String basePath;
  private Integer connectionTimeoutMillis;
  private Integer connectionRetryCount;
  private Integer conntectionRetrySleepMillis;
  private InstanceSerializerFactory instanceSerializerFactory;
  private String serviceName;

  @Override
  public void unadvertise(SocketAddress serverAddress)
  {
    if (serverAddress instanceof InetSocketAddress) {
      doAdvertise((InetSocketAddress)serverAddress, false);
    }
    else {
      logger.warn("Unknown address {} of type {}", serverAddress, serverAddress.getClass().getName());
    }
  }

  @Override
  public void advertise(SocketAddress serverAddress)
  {
    if (serverAddress instanceof InetSocketAddress) {
      doAdvertise((InetSocketAddress)serverAddress, true);
    }
    else {
      logger.warn("Unknown address {} of type {}", serverAddress, serverAddress.getClass().getName());
    }
  }

  public void doAdvertise(InetSocketAddress address, boolean flag)
  {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMillis)
            .retryPolicy(new RetryNTimes(connectionRetryCount, conntectionRetrySleepMillis))
            .connectString(connectionString)
            .build();
    curatorFramework.start();

    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      ServiceDiscovery<SinkMetadata> discovery = getDiscovery(curatorFramework);
      discovery.start();
      ServiceInstance<SinkMetadata> instance = getInstance(address.getHostName(), address.getPort());
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
  public Collection<SocketAddress> discover()
  {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMillis)
            .retryPolicy(new RetryNTimes(connectionRetryCount, conntectionRetrySleepMillis))
            .connectString(connectionString)
            .build();
    curatorFramework.start();

    try {
      new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

      ServiceDiscovery<SinkMetadata> discovery = getDiscovery(curatorFramework);
      discovery.start();
      Collection<ServiceInstance<SinkMetadata>> services = discovery.queryForInstances(serviceName);
      ArrayList<SocketAddress> returnable = new ArrayList<SocketAddress>(services.size());
      for (ServiceInstance<SinkMetadata> service: services) {
        SinkMetadata payload = service.getPayload();
        returnable.add(new InetSocketAddress(payload.boundHost, payload.getBoundPort()));
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
    connectionString = context.getString("connectionString");
    connectionTimeoutMillis = context.getInteger("connectionTimeoutMillis", 1000);
    connectionRetryCount = context.getInteger("connectionRetryCount", 10);
    conntectionRetrySleepMillis = context.getInteger("connectionRetrySleepMillis", 500);
    basePath = context.getString("basePath");
    serviceName = context.getString("serviceName", "DTFlume");

    ObjectMapper om = new ObjectMapper();
    instanceSerializerFactory = new InstanceSerializerFactory(om.reader(), om.writer());
  }

  private ServiceInstance<SinkMetadata> getInstance(String boundAddress, int boundPort) throws Exception
  {
    SinkMetadata sinkMetadata = new SinkMetadata(boundAddress, boundPort);
    return ServiceInstance.<SinkMetadata>builder()
            .name(serviceName)
            .address(boundAddress)
            .port(boundPort)
            .id(boundAddress + boundPort)
            .payload(sinkMetadata)
            .build();
  }

  private ServiceDiscovery<SinkMetadata> getDiscovery(CuratorFramework curatorFramework)
  {
    return ServiceDiscoveryBuilder.builder(SinkMetadata.class)
            .basePath(basePath)
            .client(curatorFramework)
            .serializer(instanceSerializerFactory.getInstanceSerializer(
                            new TypeReference<ServiceInstance<SinkMetadata>>()
                            {
                            }
                    ))
            .build();
  }

  public final class SinkMetadata
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
