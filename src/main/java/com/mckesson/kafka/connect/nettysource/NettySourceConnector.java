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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.nettysource.utils.Version;

public class NettySourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(NettySourceConnector.class);

  protected NettySourceConnectorConfig config;
  private Class<? extends Task> taskClass;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    log.info("Start NettySourceConnector ....");
    try {
      config = new NettySourceConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start TcpSourceConnector due to configuration error", e);
    }

    String transport = config.getString(NettySourceConnectorConfig.TRANSPORT_PROTOCOL_CONFIG);
    switch (transport.toUpperCase()) {
      case "TCP":
        taskClass = TcpSourceTask.class;
        break;

      case "UDP":
        taskClass = UdpSourceTask.class;
        break;

      default:
        throw new ConnectException("Unsupported " + NettySourceConnectorConfig.TRANSPORT_PROTOCOL_CONFIG + " :" + transport);
    }

  }

  @Override
  public Class<? extends Task> taskClass() {
    return taskClass;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {

    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(config.originalsStrings());
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopped NettySourceConnector");
  }

  @Override
  public ConfigDef config() {
    return NettySourceConnectorConfig.CONFIG_DEF;
  }
}
