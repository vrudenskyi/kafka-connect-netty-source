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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;

public class SyslogPipelineFactory extends NettyPipelineFactory {

  private static final String CONFIG_PREFIX = NettySourceConnectorConfig.PIPELINE_FACTORY_CONFIG + ".syslog.";

  public static final String MAX_LENGTH_CONFIG = CONFIG_PREFIX + "maxLength";
  private static final int MAX_LENGTH_DEFAULT = 8192;

  public static final String STRIP_DELIMETER_CONFIG = CONFIG_PREFIX + "stripDelimiter";
  private static final Boolean STRIP_DELIMETER_DEFAULT = Boolean.TRUE;

  public static final String FAIL_FAST_CONFIG = CONFIG_PREFIX + "failFast";
  private static final Boolean FAIL_FAST_DEFAULT = Boolean.FALSE;

  public static final String DELIMETERS_CONFIG = CONFIG_PREFIX + "delimeters";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(MAX_LENGTH_CONFIG, ConfigDef.Type.INT, MAX_LENGTH_DEFAULT, ConfigDef.Importance.MEDIUM, "Max Message Length")
      .define(STRIP_DELIMETER_CONFIG, ConfigDef.Type.BOOLEAN, STRIP_DELIMETER_DEFAULT, ConfigDef.Importance.MEDIUM, "whether the decoded frame should strip out the delimiter or not")
      .define(FAIL_FAST_CONFIG, ConfigDef.Type.BOOLEAN, FAIL_FAST_DEFAULT, ConfigDef.Importance.MEDIUM, "see LineBasedFrameDecoder javadoc")
      .define(DELIMETERS_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM, "list of delimeter strings");

  private int maxLength = 8192;
  private boolean stripDelimiter = true;
  private boolean failFast = false;
  private List<ChannelBuffer> delimeters;

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

  }

  @Override
  public LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySourceConnectorConfig conf) {

    FrameDecoder framer;
    if (this.delimeters == null || this.delimeters.size() == 0) {
      framer = new LineBasedFrameDecoder(maxLength, stripDelimiter, failFast);
    } else {
      framer = new DelimiterBasedFrameDecoder(maxLength, stripDelimiter, failFast, delimeters.toArray(new ChannelBuffer[0]));
    }

    LinkedHashMap<String, ChannelHandler> defaultHandlers = new LinkedHashMap<>();
    defaultHandlers.put("framer", framer);
    defaultHandlers.put("decoder", new StringDecoder());
    SyslogRecordHandler handler = new SyslogRecordHandler();
    handler.setTopic(topic);
    handler.setRecordQueue(messageQueue);
    defaultHandlers.put("recordHandler", handler);
    return defaultHandlers;
  }

}
