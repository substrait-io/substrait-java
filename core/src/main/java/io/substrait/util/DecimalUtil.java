package io.substrait.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A set of utility methods to deal with convertion of decimal values. Part of code is adopted from
 * Apache Arrow /java/vector/src/main/java/org/apache/arrow/vector/util/DecimalUtility.java
 */
public class DecimalUtil {
  private static final byte zero = 0;
  private static final byte minus_one = -1;

  private static final long[] POWER_OF_10 = {
    1l,
    10l,
    100l,
    1000l,
    10_000l,
    100_000l,
    1_000_000l,
    10_000_000l,
    100_000_000l,
    1_000_000_000l,
    10_000_000_000l,
    100_000_000_000l,
    1_000_000_000_000l,
    10_000_000_000_000l,
    100_000_000_000_000l,
    1_000_000_000_000_000l,
    10_000_000_000_000_000l,
    100_000_000_000_000_000l // long max = 9,223,372,036,854,775,807
  };

  /**
   * Given an input of little-endian twos-complement with byeWidth bytes of a scaled big integer,
   * convert it back to BigDecimal. This method is the opposite of encodeDecimalIntoBytes()
   *
   * @param value
   * @param scale
   * @param byteWidth
   * @return
   */
  public static BigDecimal getBigDecimalFromBytes(byte[] value, int scale, int byteWidth) {
    byte[] reversed = new byte[value.length];
    for (int i = 0; i < byteWidth; i++) {
      reversed[byteWidth - 1 - i] = value[i];
    }
    BigInteger unscaledValue = new BigInteger(reversed);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Given a big decimal and its scale, convert into a scaled big integer, and then convert the
   * scaled big integer into little-endian twos-complement with byeWidth bytes.
   *
   * @param decimal
   * @param scale
   * @param byteWidth
   * @return
   */
  public static byte[] encodeDecimalIntoBytes(BigDecimal decimal, int scale, int byteWidth) {
    BigDecimal scaledDecimal = decimal.multiply(powerOfTen(scale));
    byte[] bytes = scaledDecimal.toBigInteger().toByteArray();
    if (bytes.length > byteWidth) {
      throw new UnsupportedOperationException(
          "Decimal size greater than " + byteWidth + " bytes: " + bytes.length);
    }
    byte[] encodedBytes = new byte[byteWidth];
    byte padByte = bytes[0] < 0 ? minus_one : zero;
    // Decimal stored as native-endian, need to swap data bytes if LE
    byte[] bytesLE = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      bytesLE[i] = bytes[bytes.length - 1 - i];
    }

    int destIndex = 0;
    for (int i = 0; i < bytes.length; i++) {
      encodedBytes[destIndex++] = bytesLE[i];
    }

    for (int j = bytes.length; j < byteWidth; j++) {
      encodedBytes[destIndex++] = padByte;
    }
    return encodedBytes;
  }

  private static BigDecimal powerOfTen(int scale) {
    if (scale < POWER_OF_10.length) {
      return new BigDecimal(POWER_OF_10[scale]);
    } else {
      int length = POWER_OF_10.length;
      BigDecimal bd = new BigDecimal(POWER_OF_10[length - 1]);

      for (int i = length - 1; i < scale; i++) {
        bd = bd.multiply(new BigDecimal(10));
      }
      return bd;
    }
  }
}
