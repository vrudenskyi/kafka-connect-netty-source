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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringRecordHandler extends SourceRecordHandler {

  static final Logger LOG = LoggerFactory.getLogger(StringRecordHandler.class);

  private final boolean skipBlank;

  public StringRecordHandler() {
    this.skipBlank = true;
  }

  public StringRecordHandler(boolean skipBlank) {
    this.skipBlank = skipBlank;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    // Cast to a String first.
    String msg = (String) e.getMessage();
    if (skipBlank && StringUtils.isBlank(msg)) {
      return;
    }
    Map<String, ?> sourcePartition = new HashMap<>();
    Map<String, ?> sourceOffset = new HashMap<>();
    //

    if (recordQueue == null) {
      throw new IllegalStateException("recordQueue is not configured");
    }

    SourceRecord srcRec = new SourceRecord(sourcePartition, sourceOffset, topic, null, msg);

    //add remoteAddr to headers
    SocketAddress remoteAddr = e.getRemoteAddress();
    if (remoteAddr == null) {
      remoteAddr = e.getChannel().getRemoteAddress();
    }
    if (remoteAddr == null) {
      remoteAddr = ctx.getChannel().getRemoteAddress();
    }
    if (remoteAddr != null) {
      if (remoteAddr instanceof InetSocketAddress) {
        srcRec.headers().add("remoteHost", new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) remoteAddr).getHostString()));
        srcRec.headers().add("remotePort", new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) remoteAddr).getPort()));
      }
      srcRec.headers().add("remoteAddress", new SchemaAndValue(Schema.STRING_SCHEMA, remoteAddr.toString()));
    }

    //add 'transportProtocol' header
    Channel channel = e.getChannel();
    if (channel == null) {
      channel = ctx.getChannel();
    }
    if (channel != null) {

      if (channel instanceof ServerSocketChannel) {
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "tcp"));
      } else if (channel instanceof DatagramChannel) {
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "udp"));
      } else {
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, channel.toString()));
      }
    }

    SocketAddress localAddr = e.getChannel().getLocalAddress();
    if (localAddr == null) {
      localAddr = ctx.getChannel().getLocalAddress();
    }
    if (localAddr != null) {
      if (localAddr instanceof InetSocketAddress) {
        srcRec.headers().add("localHost", new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) localAddr).getHostString()));
        srcRec.headers().add("localPort", new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) localAddr).getPort()));
      }
      srcRec.headers().add("localAddress", new SchemaAndValue(Schema.STRING_SCHEMA, localAddr.toString()));
    }
    recordQueue.add(srcRec);
    checkQueueCapacity();

  }

}
