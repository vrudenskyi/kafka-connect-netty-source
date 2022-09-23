/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vrudenskyi.kafka.connect.nettysource;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vrudensk.kafka.connect.utils.QueueBatchConfig;
import com.vrudenskyi.kafka.connect.nettysource.utils.Version;

public abstract class NettySourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(NettySourceTask.class);

  private ChannelGroup chGroup;
  private ChannelFactory workerChFactory;
  private ChannelFactory statusChFactory;
  private ChannelPipelineFactory pipelineFactory;

  protected NettySourceConnectorConfig connConfig;
  protected String tskName = "Not started NettySourceTask";

  private BlockingQueue<SourceRecord> eventsQueue;
  private int queueBatchSize;
  private int queueCapacity;
  private long queueTimeout;

  private long pollInterval;
  private AtomicBoolean stop;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info("Starting NettySourceTask...");
    try {
      connConfig = new NettySourceConnectorConfig(props);
      final int workingThreads = connConfig.getInt(NettySourceConnectorConfig.THREADS_CONFIG);
      final Integer port = connConfig.getInt(NettySourceConnectorConfig.PORT_CONFIG);
      List<Integer> ports = connConfig.getList(NettySourceConnectorConfig.PORTS_CONFIG).stream().map(p -> Integer.valueOf(p)).collect(Collectors.toList());
      if (port != null) {
        ports.add(0, port);
      }
      if (ports.size() == 0) {
        throw new ConnectException("Port is not configured.");
      }

      // configure task representation String
      this.tskName = new StringBuilder().append(this.getClass().getSimpleName() + " (port:").append(StringUtils.join(ports, ",")).append(" topic: ")
          .append(connConfig.getString(NettySourceConnectorConfig.TOPIC_CONFIG)).append(")").toString();

      pollInterval = connConfig.getLong(NettySourceConnectorConfig.POLL_INTERVAL_CONFIG);
      final InetAddress bindAddress = InetAddress
          .getByName(connConfig.getString(NettySourceConnectorConfig.BIND_ADDRESS_CONFIG));

      this.queueBatchSize = connConfig.getInt(QueueBatchConfig.QUEUE_BATCH_CONFIG);
      this.queueTimeout = connConfig.getLong(QueueBatchConfig.QUEUE_TIMEOUT_CONFIG);
      this.eventsQueue = connConfig.getConfiguredInstance(QueueBatchConfig.QUEUE_CLASS_CONFIG, BlockingQueue.class);
      if (this.eventsQueue == null) {
        this.queueCapacity = connConfig.getInt(QueueBatchConfig.QUEUE_CAPACITY_CONFIG);
        this.eventsQueue = new LinkedBlockingQueue<>(queueCapacity);
      }

      Class<?> pipelineClass = connConfig.getClass(NettySourceConnectorConfig.PIPELINE_FACTORY_CLASS_CONFIG);
      if (pipelineClass == null) {
        pipelineClass = getDefaultPipelineClass();
      }

      pipelineFactory = (ChannelPipelineFactory) Utils.newInstance(pipelineClass);
      if (pipelineFactory instanceof NettyPipelineFactory) {
        ((NettyPipelineFactory) pipelineFactory).setMessageQueue(eventsQueue);
      }
      if (pipelineFactory instanceof Configurable) {
        ((Configurable) pipelineFactory).configure(props);
      }

      chGroup = new DefaultChannelGroup();
      this.workerChFactory = createWorkerChannelFactory(workingThreads);

      Channel workerCh = createWorkerChannel(bindAddress, ports, this.workerChFactory, pipelineFactory);
      chGroup.add(workerCh);

      //start healthcheck tcp port if needed
      Boolean healthCheck = connConfig.getBoolean(NettySourceConnectorConfig.HEALTHCHECK_ENABLED_CONFIG);
      if (healthCheck != null && healthCheck) {
        try {
          List<Integer> statusPorts = connConfig.getList(NettySourceConnectorConfig.HEALTHCHECK_PORTS_CONFIG).stream().map(p -> Integer.valueOf(p)).collect(Collectors.toList());
          Integer statusPort = connConfig.getInt(NettySourceConnectorConfig.HEALTHCHECK_PORT_CONFIG);
          if (statusPort != null) {
            statusPorts.add(0, statusPort);
          }

          if (statusPorts.size() == 0) {
            statusPorts.addAll(ports);
          }

          String statusAddr = connConfig.getString(NettySourceConnectorConfig.HEALTHCHECK_BIND_ADDRESS_CONFIG);
          if (StringUtils.isBlank(statusAddr)) {
            statusAddr = connConfig.getString(NettySourceConnectorConfig.BIND_ADDRESS_CONFIG);
          }

          this.statusChFactory = createStatusChannelFactory();
          Channel statusChannel = createStatusChannel(selectTcpSocketAddress(InetAddress.getByName(statusAddr), statusPorts), this.statusChFactory);
          chGroup.add(statusChannel);
          log.info("Started healthcheck listener on {}:{}", statusAddr, statusPort);

        } catch (Exception e) {
          log.error("Failed to start tcp status listener", e);
        }

      }

    } catch (Exception e) {
      throw new ConnectException("NettySourceTask failed to start for topic: ", e);
    }

    stop = new AtomicBoolean(false);
    log.info("{} started.", this.tskName);

  }

  /**
   * Creates address for the first available port
   * @param bindAddress
   * @param ports
   * @return
   */
  protected InetSocketAddress selectTcpSocketAddress(InetAddress bindAddress, List<Integer> ports) {
    log.debug("selecting TCP port for: {} from {}", bindAddress, ports);
    for (Integer port : ports) {
      try (ServerSocket ss = new ServerSocket(port, 0, bindAddress)) {
        log.debug("\tTCP Port Selected: {}", port);
        return new InetSocketAddress(bindAddress, port);
      } catch (Exception e) {
        log.debug("\tTCP Port In Use: {}", port);
      }

    }

    throw new ConnectException("[" + this.tskName + "]: all port are in use. ");

  }

  protected InetSocketAddress selectUdpSocketAddress(InetAddress bindAddress, List<Integer> ports) {
    log.debug("selecting UDP port for: {} from {}", bindAddress, ports);
    for (Integer port : ports) {
      try (DatagramSocket ds = new DatagramSocket(port, bindAddress)) {
        log.debug("\tUDP Port Selected: {}", port);
        return new InetSocketAddress(bindAddress, port);
      } catch (Exception e) {
        log.debug("\tUDP Port In Use: {}", port);
      }

    }

    throw new ConnectException("[" + this.tskName + "]: all port are in use. ");

  }

  protected abstract Class<? extends ChannelPipelineFactory> getDefaultPipelineClass();

  protected abstract ChannelFactory createWorkerChannelFactory(int workingThreads);

  protected abstract Channel createWorkerChannel(InetAddress bindAddress, List<Integer> ports, ChannelFactory chFactory, ChannelPipelineFactory pipelineFactory);

  protected ChannelFactory createStatusChannelFactory() {
    return new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 1);
  }

  protected Channel createStatusChannel(final InetSocketAddress addr, ChannelFactory chFactory) {
    ServerBootstrap bootstrap = new ServerBootstrap(chFactory);
    bootstrap.setPipeline(Channels.pipeline(new LoggingHandler()));
    // Bind and start to accept incoming connections.
    Channel ch = bootstrap.bind(addr);
    return ch;
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    if (stop == null || stop.get()) {
      log.debug("{}: is not started or stopped. Exit poll immediately", this.tskName);
      int queueSize = eventsQueue.size();
      if (queueSize > 0) {
        log.debug("Cached queue size: {}. Drain all before exit.", queueSize);
        List<SourceRecord> records = new ArrayList<>(queueSize);
        this.eventsQueue.drainTo(records, queueSize);
        this.eventsQueue.clear();
        this.eventsQueue = null;
        return records;
      }

    }

    int expectedSize = Math.min(this.eventsQueue.size(), this.queueBatchSize);
    List<SourceRecord> records = new ArrayList<>(expectedSize);
    this.eventsQueue.drainTo(records, this.queueBatchSize);

    if (records.size() >= this.queueBatchSize) {
      log.warn("Drained {} recs from queue and reached max batchsize immediately!. Current eventsQueueSize: {}", records.size(), this.eventsQueue.size());
      return records;
    }

    long timeToStop = System.currentTimeMillis() + this.pollInterval;
    while (true) {
      SourceRecord rec = this.eventsQueue.poll(queueTimeout, TimeUnit.MILLISECONDS);
      if (rec != null) {
        records.add(rec);
      }

      if (stop.get()) {
        log.info("Stop signal received. Exit immediately. Current queue size: {}. Drain all before exit.", eventsQueue.size());
        int queueSize = eventsQueue.size();
        this.eventsQueue.drainTo(records, queueSize);
        this.eventsQueue.clear();
        this.eventsQueue = null;
        break;
      }

      if ((System.currentTimeMillis() >= timeToStop) || records.size() >= this.queueBatchSize) {
        log.trace("Reached timeout or batchsize. Returning records");
        break;
      }

    }

    log.trace("Poll returned {} records of {} ({})", records.size(), this.queueBatchSize, tskName);
    return records;
  }

  @Override
  public void stop() {
    log.debug("Stopping {}", this.tskName);
    stop.set(true);
    if (this.pipelineFactory instanceof Closeable) {
      try {
        ((Closeable) (this.pipelineFactory)).close();
      } catch (IOException e) {
        log.warn("Failed to close pipeline factory", e);
      }
    }
    chGroup.close().awaitUninterruptibly();
    workerChFactory.releaseExternalResources();
    if (statusChFactory != null) {
      statusChFactory.releaseExternalResources();
    }
    log.debug("Stopped {}", this.tskName);
    tskName = null;
  }
}
