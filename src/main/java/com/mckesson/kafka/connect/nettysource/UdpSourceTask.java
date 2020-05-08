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
package com.mckesson.kafka.connect.nettysource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramWorkerPool;

public class UdpSourceTask extends NettySourceTask {

  public static final String UPD_OPTIONS = "transport.protocol.udp.";

  @Override
  protected ChannelFactory createWorkerChannelFactory(int workingThreads) {
    NioDatagramWorkerPool workerPool = new NioDatagramWorkerPool(Executors.newCachedThreadPool(), workingThreads);
    return new NioDatagramChannelFactory(workerPool);
  }

  @Override
  protected Channel createWorkerChannel(InetAddress bindAddress, List<Integer> ports, ChannelFactory chFactory, ChannelPipelineFactory pipelineFactory) {
    InetSocketAddress addr = selectUdpSocketAddress(bindAddress, ports);
    ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(chFactory);

    bootstrap.setOption("receiveBufferSize", 2048);

    Map<String, Object> options = this.connConfig.originalsWithPrefix(UPD_OPTIONS);
    if (options != null && options.size() > 0) {
      bootstrap.setOptions(options);
    }
    //set default
    bootstrap.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(Integer.valueOf(bootstrap.getOption("receiveBufferSize").toString())));

    bootstrap.setPipelineFactory(pipelineFactory);
    // Bind and start to accept incoming connections.
    Channel ch = bootstrap.bind(addr);
    return ch;
  }

  @Override
  protected Class<? extends ChannelPipelineFactory> getDefaultPipelineClass() {
    return DefaultUdpPipelineFactory.class;
  }
}
