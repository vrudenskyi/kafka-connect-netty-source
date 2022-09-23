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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HttpHeaders;

public class HttpRequestRecordHandler extends SourceRecordHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestRecordHandler.class);

  public static final String AUTH_CONFIG = "authValue";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(AUTH_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, "Required Authorization header value");

  private Password authorizationLine;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    authorizationLine = conf.getPassword(AUTH_CONFIG);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    DefaultHttpRequest msg = (DefaultHttpRequest) e.getMessage();
    if (msg == null) {
      return;
    }

    final HttpResponse response;
    if (authorizationLine == null ||
        (msg.headers().contains(HttpHeaders.AUTHORIZATION) && authorizationLine.value().equals(msg.headers().get(HttpHeaders.AUTHORIZATION)))) {
      response = createResponse(HttpResponseStatus.OK);

      Map<String, SchemaAndValue> extraHeaders = new HashMap<>();
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
          extraHeaders.put("remoteHost", new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) remoteAddr).getHostString()));
          extraHeaders.put("remotePort", new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) remoteAddr).getPort()));
        }
        extraHeaders.put("remoteAddress", new SchemaAndValue(Schema.STRING_SCHEMA, remoteAddr.toString()));
      }

      List<SourceRecord> records = produceRecordsFromContent(msg, extraHeaders);
      LOG.debug("Queued for Topic: {}, records: {}", topic, records.size());
      recordQueue.addAll(records);
    } else {
      response = createResponse(HttpResponseStatus.UNAUTHORIZED);
    }

    // Write the response.
    ChannelFuture future = e.getChannel().write(response);
    future.addListener(ChannelFutureListener.CLOSE); // WE DO NOT RESPECT Keep-Alive
  }

  protected HttpResponse createResponse(HttpResponseStatus status) {
    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
  }

  protected List<SourceRecord> produceRecordsFromContent(DefaultHttpRequest msg, Map<String, SchemaAndValue> extraHeaders) throws Exception {
    ChannelBuffer dataBuffer = msg.getContent();
    if (dataBuffer == null || !dataBuffer.readable()) {
      return Collections.emptyList();
    }
    byte[] data;
    if (dataBuffer.hasArray()) {
      data = dataBuffer.array();
    } else {
      int readableBytes = dataBuffer.readableBytes();
      data = new byte[readableBytes];
      dataBuffer.getBytes(0, data);
    }

    Map<String, ?> sourcePartition = new HashMap<>();
    Map<String, ?> sourceOffset = new HashMap<>();
    LOG.trace("Read bytes:{}", data.length);
    SourceRecord rec = new SourceRecord(sourcePartition, sourceOffset, topic, null, data);
    if (extraHeaders != null && !extraHeaders.isEmpty()) {
      for (Entry<String, SchemaAndValue> e : extraHeaders.entrySet()) {
        rec.headers().add(e.getKey(), e.getValue());
      }
    }
    return Arrays.asList(rec);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("ERROR: {},  {} ", ctx.getName(), e.getChannel(), e.getCause());
    e.getChannel().close();
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    super.channelConnected(ctx, e);
    LOG.trace("Connected from {}", ctx.getChannel().getRemoteAddress());

  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // TODO Auto-generated method stub
    super.channelDisconnected(ctx, e);
    LOG.trace("Disconnected {}", ctx.getChannel().getRemoteAddress());
  }

}
