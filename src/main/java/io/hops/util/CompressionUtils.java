package io.hops.util;

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

  public static byte[] decompress(byte[] data)
      throws IOException, DataFormatException {
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
