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

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for message handling ChannelHandlers
 */
public class SourceRecordHandler extends SimpleChannelUpstreamHandler implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRecordHandler.class);

  public static final String CLOSE_ON_QUEUE_OVERFLOW_CONFIG = "closeOnQueueOverflow";
  public static final Boolean CLOSE_ON_QUEUE_OVERFLOW_DEFAULT = Boolean.FALSE;

  public static final String QUEUE_FILLED_THRESHOLD_CONFIG = "queueFilledThreshold";
  public static final int QUEUE_FILLED_THRESHOLD_DEFAULT = 95;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CLOSE_ON_QUEUE_OVERFLOW_CONFIG, ConfigDef.Type.BOOLEAN, CLOSE_ON_QUEUE_OVERFLOW_DEFAULT, ConfigDef.Importance.LOW, "Close channel on queue reached filled threshold")
      .define(QUEUE_FILLED_THRESHOLD_CONFIG, ConfigDef.Type.INT, QUEUE_FILLED_THRESHOLD_DEFAULT, ConfigDef.Importance.LOW, "Queue filled treshold in percent. default: 95");

  protected BlockingQueue<SourceRecord> recordQueue;
  protected String topic;
  private boolean closeOnQueueOverflow = false;
  private int queueFilledThreshold = 95;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    closeOnQueueOverflow = conf.getBoolean(CLOSE_ON_QUEUE_OVERFLOW_CONFIG);
    queueFilledThreshold = conf.getInt(QUEUE_FILLED_THRESHOLD_CONFIG);
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setRecordQueue(BlockingQueue<SourceRecord> queue) {
    this.recordQueue = queue;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    if (e.getCause() instanceof QueueOverflowException) { //handle capacity overflow
      LOG.trace("QueueOverflowException: closing channel immidiately");
      ChannelFuture cf = e.getChannel().close().syncUninterruptibly();
    }
    ctx.sendUpstream(e);
  }

  protected void checkQueueCapacity() {
    //check capacity: through exception if reached % of capacity
    int denominator = (100 / (100 - queueFilledThreshold));
    int minRemainingCapacity = 1 + (this.recordQueue.remainingCapacity() + this.recordQueue.size()) / denominator;
    if (recordQueue.remainingCapacity() < minRemainingCapacity) {
      LOG.warn("Queue size reached 95% of capacity({}) will through exception to close connection", this.recordQueue.remainingCapacity() + this.recordQueue.size());
      if (closeOnQueueOverflow) {
        throw new QueueOverflowException();
      }
    }
  }

  public class QueueOverflowException extends RuntimeException {
  }

}
