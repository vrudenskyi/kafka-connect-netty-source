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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.vrudensk.kafka.connect.utils.QueueBatchConfig;

public class NettySourceConnectorConfig extends AbstractConfig {

  public static final String BIND_ADDRESS_CONFIG = "bind.address";
  public static final String BIND_ADDRESS_DEFAULT = "0.0.0.0";

  public static final String PORT_CONFIG = "port";
  public static final String PORTS_CONFIG = "ports";

  public static final String TRANSPORT_PROTOCOL_CONFIG = "transport.protocol";
  public static final String TRANSPORT_PROTOCOL_DEFAULT = "tcp";

  public static final String HEALTHCHECK_ENABLED_CONFIG = "healthcheck.enabled";
  public static final Boolean HEALTHCHECK_ENABLED_DEFAULT = Boolean.FALSE;
  public static final String HEALTHCHECK_BIND_ADDRESS_CONFIG = "healthcheck.bind.address";
  public static final String HEALTHCHECK_PORT_CONFIG = "healthcheck.port";
  public static final String HEALTHCHECK_PORTS_CONFIG = "healthcheck.ports";

  public static final String POLL_INTERVAL_CONFIG = "poll.interval";
  public static final Long POLL_INTERVAL_DEFAULT = 5000L;

  public static final String TOPIC_CONFIG = "topic";
  

  public static final String PIPELINE_FACTORY_CONFIG = "pipeline.factory";
  public static final String PIPELINE_FACTORY_CLASS_CONFIG = PIPELINE_FACTORY_CONFIG + ".class";
  public static final String PIPELINE_FACTORY_HANDLERS_CONFIG = PIPELINE_FACTORY_CONFIG + ".handlers";
  public static final String THREADS_CONFIG = "threads";
  public static final int THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();

  public static final String SSL_ENABLED_CONFIG = "ssl.enabled";
  public static final Boolean SSL_ENABLED_DEFAULT = Boolean.FALSE;

  public static final String SSL_KEY_ALIAS_CONFIG = "ssl.key.alias";

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();

    final String group = "NettySourceConnector";
    int order = 0;
    configDef
        .define(BIND_ADDRESS_CONFIG, Type.STRING, BIND_ADDRESS_DEFAULT, Importance.HIGH, "Bind addresses", group,
            ++order, Width.LONG, "Bind addresses")
        .define(PORT_CONFIG, Type.INT, null, Importance.HIGH, "Listening port", group, ++order, Width.LONG,
            "Listening port")
        .define(PORTS_CONFIG, Type.LIST, Collections.emptyList(), Importance.MEDIUM, "List of listening ports", group, ++order, Width.LONG,
            "List of listening ports")
        .define(TRANSPORT_PROTOCOL_CONFIG, Type.STRING, TRANSPORT_PROTOCOL_DEFAULT, Importance.HIGH, "Type of transport: TCP, UDP", group, ++order, Width.LONG,
            "Type of transport: TCP, UDP")
        .define(THREADS_CONFIG, Type.INT, THREADS_DEFAULT, Importance.MEDIUM, "number of worker threads", group,
            ++order, Width.LONG, "number of worker threads")
        .define(POLL_INTERVAL_CONFIG, Type.LONG, POLL_INTERVAL_DEFAULT, Importance.LOW,
            "sleep time in millis between polls", group, ++order, Width.LONG, "sleep time in millis between polls")
        .define(PIPELINE_FACTORY_CLASS_CONFIG, Type.CLASS, null, Importance.HIGH,
            "Netty pipeline factory", group, ++order, Width.LONG, "Netty pipeline factory")
        .define(PIPELINE_FACTORY_HANDLERS_CONFIG, Type.LIST, Collections.emptyList(), Importance.LOW,
            "List of pipeline handlers", group, ++order, Width.LONG, "List of pipeline handlers. Used to override pipeline defaults")
        .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Kafka topic", group, ++order, Width.LONG,
            "Kafka topic")
        .define(SSL_ENABLED_CONFIG, Type.BOOLEAN, SSL_ENABLED_DEFAULT, Importance.MEDIUM,
            "Enable SSL. Adds SSL handler", group, ++order, Width.LONG, "Enable SSL. Adds SSL handler")
        .define(SSL_KEY_ALIAS_CONFIG, Type.STRING, null, Importance.MEDIUM,
            "Key alias to use", group, ++order, Width.LONG, "Key alias to use")
        .define(HEALTHCHECK_ENABLED_CONFIG, Type.BOOLEAN, HEALTHCHECK_ENABLED_DEFAULT, Importance.MEDIUM, "Open tcp port to task status check", group,
            ++order, Width.LONG, "Enable tcp status check port")
        .define(HEALTHCHECK_BIND_ADDRESS_CONFIG, Type.STRING, null, Importance.MEDIUM, "Bind addresses for tcp status check", group,
            ++order, Width.LONG, "Bind addresses for tcp status check")
        .define(HEALTHCHECK_PORT_CONFIG, Type.INT, null, Importance.MEDIUM, "TCP port for status. Default: same as port", group, ++order, Width.LONG,
            " Default: same as port")
        .define(HEALTHCHECK_PORTS_CONFIG, Type.LIST, Collections.emptyList(), Importance.MEDIUM, "TCP ports for status. Default: same as ports", group, ++order, Width.LONG,
            " Default: same as ports")
        .withClientSslSupport();
    // add queue config
    QueueBatchConfig.addConfig(configDef);

    return configDef;
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public NettySourceConnectorConfig(Map<String, ?> props) {
    super(CONFIG_DEF, props);
  }

  public NettySourceConnectorConfig(ConfigDef confDef, Map<String, ?> props) {
    super(confDef, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }
}
