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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;


/**
 * Factory for ChannelHandlers
 */
public class ChannelHandlerFactory {

  public static Map<String, Function<Map<String, Object>, ChannelHandler>> handlersMap = new HashMap<>();
  public static Map<String, Function<Map<String, Object>, LinkedHashMap<String, ChannelHandler>>> setsMap = new HashMap<>();

  /**
   * Default function to create ChannelHandlers. Used for handlers with no-params  constructor
   */
  private static final Function<Map<String, Object>, ChannelHandler> DEFAULT_CHANNEL_CREATOR = config -> {
    String clazzName = config.get("class").toString();
    Object clazzInstance = null;
    try {
      clazzInstance = Class.forName(clazzName).newInstance();
    } catch (Exception e) {
      throw new ConnectException("Failed to create ChannelHandler:" + clazzName);
    }

    if (clazzInstance != null && clazzInstance instanceof ChannelHandler) {
      return (ChannelHandler) clazzInstance;
    }

    throw new ConnectException("Class in not instance of ChannelHandler:" + clazzName);

  };

  static {
    /**
     * LineBasedFrameDecoder
     */
    handlersMap.put("org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder", config -> {
      int maxLength = Integer.valueOf(config.getOrDefault("maxLength", "8192").toString());
      boolean stripDelimiter = Boolean.parseBoolean((config.getOrDefault("stripDelimiter", "true").toString()));
      boolean failFast = Boolean.parseBoolean((config.getOrDefault("failFast", "false").toString()));
      return new LineBasedFrameDecoder(maxLength, stripDelimiter, failFast);
    });
    
    /**
     * HttpRequestDecoder
     * {@code maxInitialLineLength (4096}}, {@code maxHeaderSize (8192)}, and
     * {@code maxChunkSize (8192)}.
     */
    handlersMap.put("org.jboss.netty.handler.codec.http.HttpRequestDecoder", config -> {
      int maxInitialLineLength = Integer.valueOf(config.getOrDefault("maxInitialLineLength", "4096").toString());
      int maxHeaderSize = Integer.valueOf(config.getOrDefault("maxHeaderSize", "8192").toString());
      int maxChunkSize = Integer.valueOf(config.getOrDefault("maxChunkSize", "8192").toString());
      return new HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize);
    });    
    
    /**
     * HttpChunkAggregator
     * {@code maxContentLength (1048576}}
     */
    handlersMap.put("org.jboss.netty.handler.codec.http.HttpChunkAggregator", config -> {
      int maxContentLength = Integer.valueOf(config.getOrDefault("maxContentLength", "1048576").toString());
      return new HttpChunkAggregator(maxContentLength);
    });    
    
    
  }

  public static ChannelHandler createHandler(Map<String, Object> config) {
    if (!config.containsKey("class")) {
      return null;
    }
    return handlersMap.getOrDefault(config.get("class"), DEFAULT_CHANNEL_CREATOR).apply(config);
  }

  
  
  

}
