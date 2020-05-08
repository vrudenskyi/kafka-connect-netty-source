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

import java.io.Closeable;
import java.io.IOException;
import java.security.KeyStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.utils.SslUtils;

public abstract class NettyPipelineFactory implements ChannelPipelineFactory, Configurable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(NettyPipelineFactory.class);

  protected NettySourceConnectorConfig config;
  protected BlockingQueue<SourceRecord> messageQueue;
  protected String topic;
  protected boolean sslEnabled = false;

  public abstract LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySourceConnectorConfig conf);

  public void setMessageQueue(BlockingQueue<SourceRecord> messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline pipeline = Channels.pipeline();

    if (sslEnabled) {
      SSLContext sslContext;
      try {
        sslContext = createSSLContext(config);
      } catch (Exception e) {
        throw new ConnectException("Failed to initilize SSL", e);
      }

      SslHandler sslHandler = new SslHandler(sslContext.createSSLEngine());
      sslHandler.getEngine().setUseClientMode(false);
      pipeline.addLast("ssl", sslHandler);
    }

    configureHandlers(config, pipeline);

    return pipeline;

  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new NettySourceConnectorConfig(configs);
    this.topic = config.getString(NettySourceConnectorConfig.TOPIC_CONFIG);
    this.sslEnabled = config.getBoolean(NettySourceConnectorConfig.SSL_ENABLED_CONFIG);
  }

  @Override
  public void close() throws IOException {
  }

  public void configureHandlers(NettySourceConnectorConfig config, ChannelPipeline pipeline) {

    // configure from defaults
    for (Entry<String, ChannelHandler> e : defaultHandlers(config).entrySet()) {
      pipeline.addLast(e.getKey(), e.getValue());
    }

    List<String> handlersList = config.getList(NettySourceConnectorConfig.PIPELINE_FACTORY_HANDLERS_CONFIG);

    if (handlersList == null || handlersList.size() == 0) {
      return;
    }
    log.debug("explicit handlers configured for: {}", handlersList);

    // update defaults if config exists
    for (String handlerName : handlersList) {
      Map<String, Object> handlerConf = config
          .originalsWithPrefix(NettySourceConnectorConfig.PIPELINE_FACTORY_HANDLERS_CONFIG + "." + handlerName + ".");
      ChannelHandler handler = ChannelHandlerFactory.createHandler(handlerConf);
      log.debug("created handler for '{}' -> {}", handlerName, handler);

      if (handler instanceof Configurable) {
        ((Configurable) handler).configure(handlerConf);
      }

      if (handler != null && handler instanceof SourceRecordHandler) {
        ((SourceRecordHandler) handler).setTopic(topic);
        ((SourceRecordHandler) handler).setRecordQueue(messageQueue);
      }
      // remove, replace or add
      if (handler == null && pipeline.get(handlerName) != null) {
        pipeline.remove(handlerName);
        log.debug("removed handler for '{}'", handlerName);
      } else if (pipeline.get(handlerName) != null) {
        pipeline.replace(handlerName, handlerName, handler);
        log.debug("replaced handler for '{}'", handlerName);
      } else {
        pipeline.addLast(handlerName, handler);
        log.debug("added handler for '{}'", handlerName);
      }
    }

  }

  private SSLContext createSSLContext(NettySourceConnectorConfig config) throws Exception {

    KeyStore keyStore = SslUtils.loadKeyStore(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
        config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
        config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));

    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).value().toCharArray());

    final String sslKeyAlias = config.getString(NettySourceConnectorConfig.SSL_KEY_ALIAS_CONFIG);

    //Replace X509ExtendedKeyManager with SniKeyManager
    KeyManager[] keyManagers = kmf.getKeyManagers();
    if (StringUtils.isNoneBlank(sslKeyAlias) && keyManagers != null && keyManagers.length > 0) {
      for (int i = 0; i < keyManagers.length; i++) {
        final KeyManager km = keyManagers[i];
        if (km instanceof X509ExtendedKeyManager) {
          keyManagers[i] = new SniKeyManager((X509ExtendedKeyManager) km, sslKeyAlias);
        }

      }
    }

    KeyStore trustStore = SslUtils.loadKeyStore(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
        config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
        config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance(config.getString(SslConfigs.SSL_PROTOCOL_CONFIG));
    sslContext.init(keyManagers, tmf.getTrustManagers(), null);

    return sslContext;
  }

}
