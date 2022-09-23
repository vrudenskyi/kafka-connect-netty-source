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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelimeterOrMaxLengthFrameDecoder extends FrameDecoder {

  private static final Logger log = LoggerFactory.getLogger(DelimeterOrMaxLengthFrameDecoder.class);

  private final ChannelBuffer[] delimiters;
  private final int maxFrameLength;
  private final boolean stripDelimiter;
  private final boolean failFast;
  private int tooLongFrameLength;

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param delimiter  the delimiter
   */
  public DelimeterOrMaxLengthFrameDecoder(int maxFrameLength, ChannelBuffer delimiter) {
    this(maxFrameLength, true, delimiter);
  }

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param stripDelimiter  whether the decoded frame should strip out the
   *                        delimiter or not
   * @param delimiter  the delimiter
   */
  public DelimeterOrMaxLengthFrameDecoder(
      int maxFrameLength, boolean stripDelimiter, ChannelBuffer delimiter) {
    this(maxFrameLength, stripDelimiter, false, delimiter);
  }

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param stripDelimiter  whether the decoded frame should strip out the
   *                        delimiter or not
   * @param failFast  If <tt>true</tt>, a {@link TooLongFrameException} is
   *                  thrown as soon as the decoder notices the length of the
   *                  frame will exceed <tt>maxFrameLength</tt> regardless of
   *                  whether the entire frame has been read.
   *                  If <tt>false</tt>, a {@link TooLongFrameException} is
   *                  thrown after the entire frame that exceeds
   *                  <tt>maxFrameLength</tt> has been read.
   * @param delimiter  the delimiter
   */
  public DelimeterOrMaxLengthFrameDecoder(
      int maxFrameLength, boolean stripDelimiter, boolean failFast,
      ChannelBuffer delimiter) {
    this(maxFrameLength, stripDelimiter, failFast, new ChannelBuffer[] {
        delimiter.slice(
            delimiter.readerIndex(), delimiter.readableBytes()) });
  }

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param delimiters  the delimiters
   */
  public DelimeterOrMaxLengthFrameDecoder(int maxFrameLength, ChannelBuffer... delimiters) {
    this(maxFrameLength, true, delimiters);
  }

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param stripDelimiter  whether the decoded frame should strip out the
   *                        delimiter or not
   * @param delimiters  the delimiters
   */
  public DelimeterOrMaxLengthFrameDecoder(
      int maxFrameLength, boolean stripDelimiter, ChannelBuffer... delimiters) {
    this(maxFrameLength, stripDelimiter, false, delimiters);
  }

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength  the maximum length of the decoded frame.
   *                        A {@link TooLongFrameException} is thrown if
   *                        the length of the frame exceeds this value.
   * @param stripDelimiter  whether the decoded frame should strip out the
   *                        delimiter or not
   * @param failFast  If <tt>true</tt>, a {@link TooLongFrameException} is
   *                  thrown as soon as the decoder notices the length of the
   *                  frame will exceed <tt>maxFrameLength</tt> regardless of
   *                  whether the entire frame has been read.
   *                  If <tt>false</tt>, a {@link TooLongFrameException} is
   *                  thrown after the entire frame that exceeds
   *                  <tt>maxFrameLength</tt> has been read.
   * @param delimiters  the delimiters
   */
  public DelimeterOrMaxLengthFrameDecoder(
      int maxFrameLength, boolean stripDelimiter, boolean failFast, ChannelBuffer... delimiters) {
    validateMaxFrameLength(maxFrameLength);
    if (delimiters == null) {
      throw new NullPointerException("delimiters");
    }
    if (delimiters.length == 0) {
      throw new IllegalArgumentException("empty delimiters");
    }

    this.delimiters = new ChannelBuffer[delimiters.length];
    for (int i = 0; i < delimiters.length; i++) {
      ChannelBuffer d = delimiters[i];
      validateDelimiter(d);
      this.delimiters[i] = d.slice(d.readerIndex(), d.readableBytes());
    }

    this.maxFrameLength = maxFrameLength;
    this.stripDelimiter = stripDelimiter;
    this.failFast = failFast;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
    // Try all delimiters and choose the delimiter which yields the shortest frame.
    int currentFrameLength = Integer.MAX_VALUE;
    ChannelBuffer selectedDelim = null;
    for (ChannelBuffer delim : delimiters) {
      int frameLength = indexOf(buffer, delim);
      if (frameLength >= 0 && frameLength < currentFrameLength) {
        currentFrameLength = frameLength;
        selectedDelim = delim;
      }
    }

    ChannelBuffer resultFrame = null;

    if (selectedDelim != null) { //frame detected from delimeter 
      int delimLength = selectedDelim.capacity();

      if (currentFrameLength > maxFrameLength) { //detected frame is too long 
        resultFrame = extractFrame(buffer, buffer.readerIndex(), maxFrameLength);
        buffer.skipBytes(maxFrameLength);
        log.warn("Too long frame detected. Consider to increase max frame length. Current lenght: {}, Max lenght: {}", currentFrameLength, maxFrameLength);
      } else { //detected frame is Ok
        if (stripDelimiter) {
          resultFrame = extractFrame(buffer, buffer.readerIndex(), currentFrameLength);
        } else {
          resultFrame = extractFrame(buffer, buffer.readerIndex(), currentFrameLength + delimLength);
        }
        buffer.skipBytes(currentFrameLength + delimLength);
      }

    } else { //frame not detected 
      if (buffer.readableBytes() > maxFrameLength) { //too much data cut frame
        resultFrame = extractFrame(buffer, buffer.readerIndex(), maxFrameLength);
        buffer.skipBytes(maxFrameLength);
        log.warn("Frame was not detected withing configured  maxFramelength {}", maxFrameLength);
      }
    }
    return resultFrame;
  }

  private void fail(ChannelHandlerContext ctx, long frameLength) {
    if (frameLength > 0) {
      Channels.fireExceptionCaught(
          ctx.getChannel(),
          new TooLongFrameException(
              "frame length exceeds " + maxFrameLength +
                  ": " + frameLength + " - discarded"));
    } else {
      Channels.fireExceptionCaught(
          ctx.getChannel(),
          new TooLongFrameException(
              "frame length exceeds " + maxFrameLength +
                  " - discarding"));
    }
  }

  /**
   * Returns the number of bytes between the readerIndex of the haystack and
   * the first needle found in the haystack.  -1 is returned if no needle is
   * found in the haystack.
   */
  private static int indexOf(ChannelBuffer haystack, ChannelBuffer needle) {
    for (int i = haystack.readerIndex(); i < haystack.writerIndex(); i++) {
      int haystackIndex = i;
      int needleIndex;
      for (needleIndex = 0; needleIndex < needle.capacity(); needleIndex++) {
        if (haystack.getByte(haystackIndex) != needle.getByte(needleIndex)) {
          break;
        } else {
          haystackIndex++;
          if (haystackIndex == haystack.writerIndex() &&
              needleIndex != needle.capacity() - 1) {
            return -1;
          }
        }
      }

      if (needleIndex == needle.capacity()) {
        // Found the needle from the haystack!
        return i - haystack.readerIndex();
      }
    }
    return -1;
  }

  private static void validateDelimiter(ChannelBuffer delimiter) {
    if (delimiter == null) {
      throw new NullPointerException("delimiter");
    }
    if (!delimiter.readable()) {
      throw new IllegalArgumentException("empty delimiter");
    }
  }

  private static void validateMaxFrameLength(int maxFrameLength) {
    if (maxFrameLength <= 0) {
      throw new IllegalArgumentException(
          "maxFrameLength must be a positive integer: " +
              maxFrameLength);
    }
  }
}
