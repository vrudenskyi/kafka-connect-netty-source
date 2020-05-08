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

import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDelimeterFrameDecoder extends FrameDecoder implements Configurable {

  public static final String DELIMITER_CONFIG = "delimiter";
  public static final String DELIMITER_DEFAULT = "\\n";

  public static final String MAX_FRAME_LENGTH_CONFIG = "maxFrameLength";
  public static final int MAX_FRAME_LENGTH_DEFAULT = 4096;

  public static final String STRIP_DELIMITER_CONFIG = "stripDelimiter";
  public static final boolean STRIP_DELIMITER_DEFAULT = true;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(DELIMITER_CONFIG, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, "delimiter")
      .define(STRIP_DELIMITER_CONFIG, ConfigDef.Type.BOOLEAN, STRIP_DELIMITER_DEFAULT, ConfigDef.Importance.LOW, "stripDelimiter")
      .define(MAX_FRAME_LENGTH_CONFIG, ConfigDef.Type.INT, MAX_FRAME_LENGTH_DEFAULT, ConfigDef.Importance.LOW, "maxFrameLength");

  private static final Logger log = LoggerFactory.getLogger(SimpleDelimeterFrameDecoder.class);

  private byte delimiter;
  private int maxFrameLength;
  private boolean stripDelimiter;

  public SimpleDelimeterFrameDecoder() {
    this(4096, true, (byte) '\n');
  }

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.stripDelimiter = conf.getBoolean(STRIP_DELIMITER_CONFIG);
    this.maxFrameLength = conf.getInt(MAX_FRAME_LENGTH_CONFIG);
    this.delimiter = StringEscapeUtils.unescapeJava(conf.getString(DELIMITER_CONFIG)).getBytes()[0];
  }

  public SimpleDelimeterFrameDecoder(int maxFrameLength, boolean stripDelimiter, byte delimiter) {
    this.delimiter = delimiter;
    this.maxFrameLength = maxFrameLength;
    this.stripDelimiter = stripDelimiter;

  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {

    int frameLen = -1;
    int counter = 0;
    for (int i = buffer.readerIndex(); i < buffer.writerIndex(); i++) {
      counter++;
      if (buffer.getByte(i) == delimiter || counter >= maxFrameLength) {
        frameLen = counter;
        break;
      }
    }
    if (frameLen < 0) {
      return null;
    }

    ChannelBuffer frame = buffer.factory().getBuffer(frameLen);
    if (stripDelimiter) {
      try {
        frame.writeBytes(buffer, buffer.readerIndex(), frameLen - 1);
      } catch (Exception e) {
        throw new RuntimeException("r=" + buffer.readerIndex() + " w=" + buffer.writerIndex() + " l=" + frameLen, e);
      }
    } else {
      frame.writeBytes(buffer, buffer.readerIndex(), frameLen);
    }
    buffer.skipBytes(frameLen);
    return frame;
  }

}
