/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionUtils {

  public static byte[] compress(byte[] data) throws IOException {
    Deflater compresser = new Deflater(Deflater.BEST_COMPRESSION);
    compresser.setInput(data);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
    compresser.finish();
    byte[] buffer = new byte[1024];
    while (!compresser.finished()) {
      int count = compresser.deflate(buffer);
      outputStream.write(buffer, 0, count);
    }
    compresser.end();
    outputStream.close();
    return outputStream.toByteArray();
  }

  public static byte[] decompress(byte[] data) throws IOException,
      DataFormatException {
    Inflater decompresser = new Inflater();
    decompresser.setInput(data);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
    byte[] buffer = new byte[1024];
    while (!decompresser.finished()) {
      int count = decompresser.inflate(buffer);
      outputStream.write(buffer, 0, count);
    }
    decompresser.end();
    outputStream.close();
    return outputStream.toByteArray();
  }
}
