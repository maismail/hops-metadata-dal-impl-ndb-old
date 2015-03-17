package se.sics.hop.metadata.ndb;

public final class NdbBoolean {
  public static final byte TRUE = 1;
  public static final byte FALSE = 0;

  public static byte convert(boolean val) {
    if (val) {
      return TRUE;
    } else {
      return FALSE;
    }
  }

  public static boolean convert(byte val) {
    if (val == TRUE) {
      return true;
    }
    if (val == FALSE) {
      return false;
    }
    throw new IllegalArgumentException();
  }
}
