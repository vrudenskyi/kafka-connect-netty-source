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

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogRecordHandler extends SourceRecordHandler {

  public static final String HOST = "host";
  public static final String FACILITY = "facility";
  public static final String DATE = "date";
  public static final String LEVEL = "level";
  public static final String MESSAGE = "message";
  public static final String CHARSET = "charset";
  public static final String REMOTE_ADDRESS = "remote_address";
  public static final String HOSTNAME = "hostname";
  public static final String TRANSPORT_PROTOCOL = "transportProtocol";

  static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("com.mckesson.kafka.connect.syslog.SyslogKey")
      .doc("This schema represents the key that is written to Kafka for syslog data. This will ensure that all data for " +
          "a host ends up in the same partition.")
      .field(
          REMOTE_ADDRESS,
          SchemaBuilder.string().doc("The ip address of the host that sent the syslog message.").build())
      .build();
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("com.mckesson.kafka.connect.syslog.SyslogValue")
      .doc("This schema represents a syslog message that is written to Kafka.")
      .field(
          DATE,
          Timestamp.builder().optional().doc("The timestamp of the message.").build())
      .field(
          FACILITY,
          SchemaBuilder.int32().optional().doc("The facility of the message.").build())
      .field(
          HOST,
          SchemaBuilder.string().optional().doc("The host of the message.").build())
      .field(
          LEVEL,
          SchemaBuilder.int32().optional().doc("The level of the syslog message as defined by [rfc5424](https://tools.ietf.org/html/rfc5424)").build())
      .field(
          MESSAGE,
          SchemaBuilder.string().optional().doc("The text for the message.").build())
      .field(
          CHARSET,
          SchemaBuilder.string().optional().doc("The character set of the message.").build())
      .field(
          REMOTE_ADDRESS,
          SchemaBuilder.string().optional().doc("The ip address of the host that sent the syslog message.").build())
      .field(
          HOSTNAME,
          SchemaBuilder.string().optional().doc("The reverse DNS of the `" + REMOTE_ADDRESS + "` field.").build())
      .field(
          TRANSPORT_PROTOCOL,
          SchemaBuilder.string().optional().doc("Transport protocol.").build())
      .build();

  private static final Logger LOG = LoggerFactory.getLogger(SyslogRecordHandler.class);

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    // Cast to a String first.
    String msg = (String) e.getMessage();

    SocketAddress remoteAddr = e.getRemoteAddress();
    if (remoteAddr == null) {
      remoteAddr = e.getChannel().getRemoteAddress();
    }
    if (remoteAddr == null) {
      remoteAddr = ctx.getChannel().getRemoteAddress();
    }
    String remoteAddress = remoteAddr == null ? null : remoteAddr.toString();

    //add 'transportProtocol' header
    String transportProtocol = null;
    Channel channel = e.getChannel();
    if (channel == null) {
      channel = ctx.getChannel();
    }
    if (channel != null) {

      if (channel instanceof ServerSocketChannel) {
        transportProtocol = "tcp";
      } else if (channel instanceof DatagramChannel) {
        transportProtocol = "udp";
      } else {
        transportProtocol = channel.toString();
      }
    }

    if (StringUtils.isBlank(msg)) {
      LOG.trace("Skipped empty message from {}", remoteAddress);
      return;
    }

    SyslogEvent event = null;
    try {
      event = new SyslogEvent(msg, null);
    } catch (Exception e1) {
      LOG.warn("Failed to parse message: '{}' from {}", msg, remoteAddress);
      event = new SyslogEvent();
      event.setMessage(msg);

    }

    Map<String, String> partition = Collections.singletonMap(HOST, event.getHost());
    Map<String, String> sourceOffset = Collections.emptyMap();

    Struct keyStruct = new Struct(this.KEY_SCHEMA)
        .put(REMOTE_ADDRESS, remoteAddress);

    Struct valueStruct = new Struct(this.VALUE_SCHEMA)
        .put(DATE, event.getDate())
        .put(FACILITY, event.getFacility())
        .put(HOST, event.getHost())
        .put(LEVEL, event.getLevel())
        .put(MESSAGE, event.getMessage())
        .put(CHARSET, event.getCharSet())
        .put(REMOTE_ADDRESS, remoteAddress)
        .put(FACILITY, event.getFacility())
        .put(TRANSPORT_PROTOCOL, transportProtocol);

    SourceRecord sourceRecord = new SourceRecord(
        partition,
        sourceOffset,
        topic,
        null,
        KEY_SCHEMA,
        keyStruct,
        VALUE_SCHEMA,
        valueStruct);
    this.recordQueue.add(sourceRecord);

    checkQueueCapacity();
  }

}
