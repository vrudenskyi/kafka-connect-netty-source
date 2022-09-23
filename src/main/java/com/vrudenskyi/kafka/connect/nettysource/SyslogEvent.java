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

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class SyslogEvent {
  private static final long serialVersionUID = 6136043067089899962L;

  public static final String DATE_FORMAT = "MMM dd HH:mm:ss yyyy";
  public static final String DATE_FORMAT_S = "MMM d HH:mm:ss yyyy";

  protected String charSet = "UTF-8";
  protected String rawString = null;
  protected byte[] rawBytes = null;
  protected int rawLength = -1;
  protected Date date = null;
  protected int level = -1;
  protected int facility = -1;
  protected String host = null;
  protected boolean isHostStrippedFromMessage = false;
  protected String message = null;
  protected InetAddress inetAddress = null;

  protected SyslogEvent() {
  }

  public SyslogEvent(final String message, InetAddress inetAddress) {
    initialize(message, inetAddress);

    parse();
  }

  public SyslogEvent(final byte[] message, int length, InetAddress inetAddress) {
    initialize(message, length, inetAddress);

    parse();
  }

  protected void initialize(final String message, InetAddress inetAddress) {
    this.rawString = message;
    this.rawLength = message.length();
    this.inetAddress = inetAddress;

    this.message = message;
  }

  protected void initialize(final byte[] message, int length, InetAddress inetAddress) {
    this.rawBytes = message;
    this.rawLength = length;
    this.inetAddress = inetAddress;
  }

  protected void parseHost() {
    int i = this.message.indexOf(' ');

    if (i > -1) {
      this.host = this.message.substring(0, i).trim();
    }
  }

  protected void parseDate() {
    int datelength = 16;
    String dateFormatS = DATE_FORMAT;
    boolean isDate8601 = false;

    if (this.message.length() > datelength) {

      // http://jira.graylog2.org/browse/SERVER-287
      if (this.message.charAt(5) == ' ') {
        datelength = 15;
        dateFormatS = DATE_FORMAT_S;
      }

      if (Character.isDigit(this.message.charAt(0))) {
        datelength = this.message.indexOf(' ') + 1;
        isDate8601 = true;
      }

      String year = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));
      String originalDate = this.message.substring(0, datelength - 1);
      String modifiedDate = originalDate + " " + year;

      DateFormat dateFormat = new SimpleDateFormat(dateFormatS, Locale.ENGLISH);
      try {
        if (!isDate8601) {
          this.date = dateFormat.parse(modifiedDate);
        } else {
          this.date = Date.from(LocalDateTime.parse(originalDate).toInstant(ZoneOffset.UTC));
        }

        this.message = this.message.substring(datelength);

      } catch (ParseException pe) {
        this.date = new Date();
      }
    }

    parseHost();
  }

  protected void parsePriority() {
    if (this.message.charAt(0) == '<') {
      int i = this.message.indexOf(">");

      if (i <= 4 && i > -1) {
        String priorityStr = this.message.substring(1, i);

        int priority = 0;
        try {
          priority = Integer.parseInt(priorityStr);
          this.facility = priority >> 3;
          this.level = priority - (this.facility << 3);

          this.message = this.message.substring(i + 1);

          parseDate();

        } catch (NumberFormatException nfe) {
          //
        }

        parseHost();
      }
    }
  }

  protected void parse() {
    if (this.message == null) {
      try {
        this.message = new String(this.rawBytes, 0, this.rawLength, charSet);
      } catch (UnsupportedEncodingException e) {
        this.message = new String(this.rawBytes);
      }
    }

    parsePriority();
  }

  public int getFacility() {
    return this.facility;
  }

  public void setFacility(int facility) {
    this.facility = facility;
  }

  public byte[] getRaw() {
    if (this.rawString != null) {
      byte[] rawStringBytes;
      try {
        rawStringBytes = rawString.getBytes(charSet);
      } catch (UnsupportedEncodingException e) {
        rawStringBytes = rawString.getBytes();
      }

      return rawStringBytes;

    } else if (this.rawBytes.length == this.rawLength) {
      return this.rawBytes;

    } else {
      byte[] newRawBytes = new byte[this.rawLength];
      System.arraycopy(this.rawBytes, 0, newRawBytes, 0, this.rawLength);

      return newRawBytes;
    }
  }

  public int getRawLength() {
    return this.rawLength;
  }

  public Date getDate() {
    return this.date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public int getLevel() {
    return this.level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public String getHost() {
    return this.host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public boolean isHostStrippedFromMessage() {
    return isHostStrippedFromMessage;
  }

  public String getMessage() {
    return this.message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getCharSet() {
    return this.charSet;
  }

  public void setCharSet(String charSet) {
    this.charSet = charSet;
  }

}
