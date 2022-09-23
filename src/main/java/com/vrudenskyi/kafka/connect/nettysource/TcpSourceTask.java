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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpSourceTask extends NettySourceTask {

  private static final Logger log = LoggerFactory.getLogger(TcpSourceTask.class);

  @Override
  protected ChannelFactory createWorkerChannelFactory(int workingThreads) {

    return new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
        workingThreads);
  }

  @Override
  protected Channel createWorkerChannel(InetAddress bindAddress, List<Integer> ports, ChannelFactory chFactory, ChannelPipelineFactory pipelineFactory) {
    InetSocketAddress addr = selectTcpSocketAddress(bindAddress, ports);
    ServerBootstrap bootstrap = new ServerBootstrap(chFactory);
     
    bootstrap.setPipelineFactory(pipelineFactory);
    // Bind and start to accept incoming connections.
    Channel ch = bootstrap.bind(addr);
    
    log.debug("started listening  on: {}", addr);
    return ch;
  }

  @Override
  protected Class<? extends ChannelPipelineFactory> getDefaultPipelineClass() {
    return DefaultTcpPipelineFactory.class;
  }
}
