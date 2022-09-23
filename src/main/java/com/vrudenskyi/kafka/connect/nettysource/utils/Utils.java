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
package com.vrudenskyi.kafka.connect.nettysource.utils;

import java.beans.PropertyDescriptor;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  public static <T> T configureInstance(T object, String base, Map<String, String> configs) {
    if (object == null) {
      return null;
    }
    for (Entry<String, String> e : configs.entrySet()) {
      if (e.getKey() != null && e.getKey().startsWith(base)) {
        String pName = e.getKey().substring(base.length());
        log.trace("for:{}, set property:{}, value{}", object, pName, e.getValue());
        setProperty(pName, e.getValue(), object);
      }
    }
    return object;
  }

  public static void setProperty(String pName, String value, Object o) {

    try {

      PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(o, pName);
      if (pd != null) {
        if (pd.getPropertyType().equals(String.class)) {
          pd.getWriteMethod().invoke(o, value);
        } else if (pd.getPropertyType().equals(Integer.class) || pd.getPropertyType().equals(Integer.TYPE)) {
          pd.getWriteMethod().invoke(o, Integer.valueOf(value));
        } else if (pd.getPropertyType().equals(Boolean.class) || pd.getPropertyType().equals(Boolean.TYPE)) {
          pd.getWriteMethod().invoke(o, Boolean.valueOf(value));
        } else if (pd.getPropertyType().equals(Long.class) || pd.getPropertyType().equals(Long.TYPE)) {
          pd.getWriteMethod().invoke(o, Long.valueOf(value));
        } else if (pd.getPropertyType().equals(Double.class) || pd.getPropertyType().equals(Double.TYPE)) {
          pd.getWriteMethod().invoke(o, Double.valueOf(value));
        } else if (pd.getPropertyType().equals(Float.class) || pd.getPropertyType().equals(Float.TYPE)) {
          pd.getWriteMethod().invoke(o, Float.valueOf(value));
        } else if (pd.getPropertyType().isArray() && (pd.getPropertyType().getComponentType().equals(Byte.class) || pd.getPropertyType().getComponentType().equals(Byte.TYPE))) {
          pd.getWriteMethod().invoke(o, value.getBytes());
        } else if (pd.getPropertyType().equals(Charset.class)) {
          pd.getWriteMethod().invoke(o, Charset.forName(value));
        } else {
          log.warn("Configuration option {} of type {} is not supported.", pName, pd.getPropertyType());
        }

      } else {
        log.warn("Configuration property {} not available for object:{}", pName, o);
      }
    } catch (Exception ex) {
      log.warn("Failed to set configuration property", ex);
    }
  }

}
