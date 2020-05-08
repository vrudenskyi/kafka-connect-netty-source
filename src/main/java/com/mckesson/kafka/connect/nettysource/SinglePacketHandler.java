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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinglePacketHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SinglePacketHandler.class);

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    Object m = e.getMessage();
    if (m == null) {
      LOG.trace("NULL message received from: {}", e.getRemoteAddress());
    }

    if (!(m instanceof ChannelBuffer)) {
      LOG.trace("NON ChannelBuffer recieved: {} from {}", m.getClass(), e.getRemoteAddress());
      ctx.sendUpstream(e);
      return;
    }

    ChannelBuffer buffer = (ChannelBuffer) m;
    if (!buffer.readable()) {
      return;
    }

    int packetSize = buffer.readableBytes();
    if (packetSize > 0) {
      ChannelBuffer msg = buffer.factory().getBuffer(packetSize);
      msg.writeBytes(buffer, buffer.readerIndex(), packetSize);
      LOG.trace("Received packet size {} bytes from {}", packetSize, e.getRemoteAddress());
      Channels.fireMessageReceived(ctx, msg, e.getRemoteAddress());
    } else {
      LOG.trace("Empty buffer received from: {}", e.getRemoteAddress());
    }

  }

}
