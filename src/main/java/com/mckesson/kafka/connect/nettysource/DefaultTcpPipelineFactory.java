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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;

public class DefaultTcpPipelineFactory extends NettyPipelineFactory {

  private static final String FRAME_CONFIG_PREFIX = NettySourceConnectorConfig.PIPELINE_FACTORY_CONFIG + ".tcp.frame.";
  public static final String MAX_LENGTH_CONFIG = FRAME_CONFIG_PREFIX + "maxLength";
  public static final int MAX_LENGTH_DEFAULT = 8192;

  public static final String STRIP_DELIMETER_CONFIG = FRAME_CONFIG_PREFIX + "stripDelimiter";
  private static final Boolean STRIP_DELIMETER_DEFAULT = Boolean.TRUE;

  public static final String FAIL_FAST_CONFIG = FRAME_CONFIG_PREFIX + "failFast";
  private static final Boolean FAIL_FAST_DEFAULT = Boolean.FALSE;

  public static final String DELIMETERS_CONFIG = FRAME_CONFIG_PREFIX + "delimeters";
  public static final List<String> DELIMETERS_DEFAULT = Arrays.asList("\\0", "\\n");

  public static final String NODATA_TIMEOUT_CONFIG = NettySourceConnectorConfig.PIPELINE_FACTORY_CONFIG + ".tcp.nodataTimeout";
  private static final Long NODATA_TIMEOUT_DEFAULT = 0L;

  //TODO: implement appropriate handlers 
  public static final String MAX_CONNECTIONS_CONFIG = NettySourceConnectorConfig.PIPELINE_FACTORY_CONFIG + ".tcp.maxConnections";
  private static final int MAX_CONNECTIONS_DEFAULT = 0;
  public static final String MAX_CONNECTIONS_PER_IP_CONFIG = NettySourceConnectorConfig.PIPELINE_FACTORY_CONFIG + ".tcp.maxConnectionsPerIP";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(MAX_LENGTH_CONFIG, ConfigDef.Type.INT, MAX_LENGTH_DEFAULT, ConfigDef.Importance.MEDIUM, "Max Message Length")
      .define(STRIP_DELIMETER_CONFIG, ConfigDef.Type.BOOLEAN, STRIP_DELIMETER_DEFAULT, ConfigDef.Importance.MEDIUM, "whether the decoded frame should strip out the delimiter or not")
      .define(FAIL_FAST_CONFIG, ConfigDef.Type.BOOLEAN, FAIL_FAST_DEFAULT, ConfigDef.Importance.MEDIUM, "see LineBasedFrameDecoder javadoc")
      .define(DELIMETERS_CONFIG, ConfigDef.Type.LIST, DELIMETERS_DEFAULT, ConfigDef.Importance.MEDIUM, "list of delimeter strings")
      .define(NODATA_TIMEOUT_CONFIG, ConfigDef.Type.LONG, NODATA_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM, "when no data was read within a certain period of time")
      .define(MAX_CONNECTIONS_CONFIG, ConfigDef.Type.INT, MAX_CONNECTIONS_DEFAULT, ConfigDef.Importance.MEDIUM, "max number of connections allowed. default 4096. set to 0 to disable")
      .define(MAX_CONNECTIONS_PER_IP_CONFIG, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM, "max number of connections per IP allowed. default: maxConnections");

  private int maxLength = 8192;
  private boolean stripDelimiter = true;
  private boolean failFast = false;
  private List<ChannelBuffer> delimeters;

  private ReadTimeoutHandler readTimeoutHandler;

  public LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySourceConnectorConfig conf) {

    LinkedHashMap<String, ChannelHandler> defaultHandlers = new LinkedHashMap<>();

    if (readTimeoutHandler != null) {
      defaultHandlers.put("nodataTimeout", readTimeoutHandler);
    }

    defaultHandlers.put("framer", new DelimeterOrMaxLengthFrameDecoder(maxLength, stripDelimiter, failFast, delimeters.toArray(new ChannelBuffer[0])));
    defaultHandlers.put("decoder", new StringDecoder());
    SourceRecordHandler handler = new StringRecordHandler();
    handler.setTopic(topic);
    handler.setRecordQueue(messageQueue);
    defaultHandlers.put("recordHandler", handler);
    return defaultHandlers;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    SimpleConfig syslogConfig = new SimpleConfig(CONFIG_DEF, configs);
    this.maxLength = syslogConfig.getInt(MAX_LENGTH_CONFIG);
    this.failFast = syslogConfig.getBoolean(FAIL_FAST_CONFIG);
    this.stripDelimiter = syslogConfig.getBoolean(STRIP_DELIMETER_CONFIG);
    List<String> dlStrings = syslogConfig.getList(DELIMETERS_CONFIG);
    if (dlStrings != null && dlStrings.size() > 0) {
      this.delimeters = new ArrayList<>(dlStrings.size());
      for (String dl : dlStrings) {
        String str = StringEscapeUtils.unescapeJava(dl);
        this.delimeters.add(new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(str.getBytes())));
      }
    }
    long readTimeout = syslogConfig.getLong(NODATA_TIMEOUT_CONFIG);
    if (readTimeout > 0) {
      this.readTimeoutHandler = new ReadTimeoutHandler(new HashedWheelTimer(), readTimeout, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.readTimeoutHandler != null) {
      this.readTimeoutHandler.releaseExternalResources();
    }
  }

}
