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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

public class HttpRequestContentRecordHandler extends HttpRequestRecordHandler {

  static final Logger log = LoggerFactory.getLogger(HttpRequestContentRecordHandler.class);

  public static final String X_KAFKA_HEADER_NAME = "X-Kafka-Header";
  public static final String X_KAFKA_TOPIC = "X-Kafka-Topic";
  public static final String X_KAFKA_KEY = "X-Kafka-Key";
  public static final String X_DATA_JSON_POINTER_KEY = "X-Data-Json-Pointer";

  public static final String KAFKA_HDR_NAME_PROTOCOL_VERSION = "httpData.protocolVersion";
  public static final String KAFKA_HDR_NAME_URI = "httpData.uri";
  public static final String KAFKA_HDR_NAME_METHOD = "httpData.method";
  public static final String KAFKA_HDR_NAME_CONTENT_TYPE = "httpData.contentType";

  public static final String TOPIC_OVERWRITE_ALLOWED_CONFIG = "topicOverwriteAllowed";
  public static final String META_ALLOWED_CONFIG = "metaAllowed";

  public static final String KEY_CONFIG = "key";
  public static final String HEADERS_CONFIG_PREFIX = "headers.";
  public static final String DATA_JSON_POINTER_CONFIG = "data.jsonPointer";
  

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(TOPIC_OVERWRITE_ALLOWED_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.HIGH, "allow overwrite topic")
      .define(META_ALLOWED_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.MEDIUM, "allow custom meta: key, headers")
      .define(KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "message key. default: null")
      .define(DATA_JSON_POINTER_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "dataPointer for json content type");

  private String configuredKey;
  private String dataJsonPointer;
  private Map<String, Object> configuredHeaders;
  private boolean metaAllowed;
  private boolean topicOverwriteAllowed;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.configuredKey = conf.getString(KEY_CONFIG);
    this.metaAllowed = conf.getBoolean(META_ALLOWED_CONFIG);
    this.topicOverwriteAllowed = conf.getBoolean(TOPIC_OVERWRITE_ALLOWED_CONFIG);
    this.configuredHeaders = conf.originalsWithPrefix(HEADERS_CONFIG_PREFIX);
    this.dataJsonPointer = conf.getString(DATA_JSON_POINTER_CONFIG);

  }

  @Override
  protected List<SourceRecord> produceRecordsFromContent(DefaultHttpRequest httpMsg, Map<String, SchemaAndValue> extraHeaders) throws Exception {

    log.debug("Produce records from request: {} {}", httpMsg.getMethod().getName(), httpMsg.getUri());
    String protocolVersion = httpMsg.getProtocolVersion().getText();
    String uriString = httpMsg.getUri();
    String httpMethod = httpMsg.getMethod().getName();
    String contentType = httpMsg.headers().get(HttpHeaders.CONTENT_TYPE);

    //record TOPIC
    String recordTopic = this.topic;
    if (topicOverwriteAllowed && httpMsg.headers().contains(X_KAFKA_TOPIC)) {
      recordTopic = httpMsg.headers().get(X_KAFKA_TOPIC);
    }

    //record KEY
    String recordKey = configuredKey;
    if (metaAllowed && httpMsg.headers().contains(X_KAFKA_KEY)) {
      recordKey = httpMsg.headers().get(X_KAFKA_KEY);
    }

    //dataJsonPointer override
    String jsonPointer = this.dataJsonPointer;
    if (metaAllowed && httpMsg.headers().contains(X_DATA_JSON_POINTER_KEY)) {
      jsonPointer = httpMsg.headers().get(X_DATA_JSON_POINTER_KEY);
    }

    //record headers
    ConnectHeaders recordHeaders = new ConnectHeaders();

    //add standard headers
    recordHeaders.addString(KAFKA_HDR_NAME_PROTOCOL_VERSION, protocolVersion);
    recordHeaders.addString(KAFKA_HDR_NAME_URI, uriString);
    recordHeaders.addString(KAFKA_HDR_NAME_METHOD, httpMethod);
    recordHeaders.addString(KAFKA_HDR_NAME_CONTENT_TYPE, contentType);
    
    //add extra headers
    if (extraHeaders != null && !extraHeaders.isEmpty()) {
      for (Entry<String, SchemaAndValue> e : extraHeaders.entrySet()) {
        recordHeaders.add(e.getKey(), e.getValue());
      }
    }

    //add headers from config 
    for (Map.Entry<String, Object> e : configuredHeaders.entrySet()) {
      recordHeaders.addString(e.getKey(), e.getValue() == null ? StringUtils.EMPTY : e.getValue().toString());
    }

    //add headers from request if allowed
    if (metaAllowed) {

      //get headers from http headers 
      for (String hdr : httpMsg.headers().getAll(X_KAFKA_HEADER_NAME)) {
        for (String pair : StringUtils.split(hdr, ';')) {
          if (StringUtils.isNotBlank(pair)) {
            int eqPos = pair.indexOf('=');
            if (eqPos > 0) {
              String key = pair.substring(0, eqPos).trim();
              String value = pair.substring(eqPos + 1).trim();
              recordHeaders.addString(key, value);
            } else {
              recordHeaders.addString(pair, StringUtils.EMPTY);
            }
          }
        }
      }
      //get headers from request params 
      URI uri = new URI(uriString);
      String queryString = uri.getQuery();
      if (StringUtils.isNotBlank(queryString)) {
        final String[] pairs = queryString.split("&");
        for (String pair : pairs) {
          final int idx = pair.indexOf("=");
          final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
          final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
          recordHeaders.addString(key, value);
        }
      }

    }

    //read msg content
    ChannelBuffer dataBuffer = httpMsg.getContent();
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

    List<Object> recordValues;
    MediaType ct = MediaType.parse(contentType);
    if (MediaType.JSON_UTF_8.is(ct)) {
      log.debug("Raw data size:{}. contentType: {}. Parse data as JSON", data.length, contentType);
      recordValues = parseJson(data, jsonPointer);
    } else if (ct.is(MediaType.ANY_TEXT_TYPE)) {
      log.debug("Raw data size:{}. contentType: {}. Parse data as TEXT", data.length, contentType);
      recordValues = parseText(data);
    } else if (ct.is(MediaType.APPLICATION_BINARY)) {
      log.debug("Raw data size:{}. contentType: {}. Parse data as BINARY", data.length, contentType);
      recordValues = Arrays.asList(data);
    } else {
      log.debug("Raw data size:{}. contentType: {}. Parse data as BINARY", data.length, contentType);
      //TODO: implement configurable record producuer by content-type, binary by default
      recordValues = Arrays.asList(data);
    }

    log.debug("Will produce {} records for: topic: {}, key:{}, headers: {}", recordValues.size(), recordTopic, recordKey, recordHeaders);

    return createRecords(recordTopic, recordKey, recordHeaders, recordValues);
  }

  private List<SourceRecord> createRecords(String topic, String msgKey, ConnectHeaders headers, List<Object> dataList) {

    List<SourceRecord> result = new ArrayList<>(dataList.size());
    for (Object value : dataList) {

      Schema valueSchema = (value instanceof String) ? Schema.STRING_SCHEMA : Schema.BYTES_SCHEMA;
      SourceRecord r = new SourceRecord(null, null, topic, null, Schema.STRING_SCHEMA, msgKey, valueSchema, value, System.currentTimeMillis(), headers);
      result.add(r);
    }

    return result;

  }

  private List<Object> parseText(byte[] data) throws IOException {
    BufferedReader logLinesReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)));

    List<Object> values = new ArrayList<>();
    String dataLine;
    while ((dataLine = logLinesReader.readLine()) != null) {
      values.add(dataLine);
    }
    logLinesReader.close();

    return values;
  }

  private List<Object> parseJson(byte[] inputData, String dataPointer) throws IOException {

    List<Object> data = Collections.emptyList();
    ObjectMapper jsonMapper = new ObjectMapper();
    JsonNode node = jsonMapper.readTree(inputData);
    JsonNode dataNode = node.at(dataPointer);
    if (dataNode.isMissingNode()) {
      log.warn("'{}'node is missing in response. Whole response to be returned", dataPointer);
      try {
        data = Arrays.asList(jsonMapper.writeValueAsString(node));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(node);
      }

    } else if (dataNode.isArray()) {
      log.debug("Extracting data from array node '{}' (size:{})", dataPointer, dataNode.size());
      data = new ArrayList<>(dataNode.size());
      for (JsonNode rec : dataNode) {
        try {
          data.add(jsonMapper.writeValueAsString(rec));
        } catch (JsonProcessingException e) {
          data.add(rec);
        }
      }

    } else {
      log.warn("{} node is not an array, returned as singe object", dataPointer);
      try {
        data = Arrays.asList(jsonMapper.writeValueAsString(dataNode));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(dataNode);
      }
    }
    return data;
  }

}
