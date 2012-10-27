package com.globalpark.fastbit;

import java.io.Writer;

public class FastbitResultRow {
  private long obj;

  public FastbitResultRow() {

  }

  public void setCursor(final long cu) {
    obj = cu;
  }

  /**
   * All Fastbit getColumnAs... functions are exported,
   * the native function use the corresponding fastbit API call,
   * but return values are mapped to JavaTypes
   */

  public short getColumnAsShort(String cname) {
    return getColumnAsShort_native(obj, cname);
  }

  public int getColumnAsUShort(String cname) {
    return getColumnAsShort_native(obj, cname);
  }

  public int getColumnAsInt(String cname) {
    return getColumnAsInt_native(obj, cname);
  }

  /**
   * There is no uint in Java, so we use long here *
   */
  public long getColumnAsUInt(String cname) {
    return getColumnAsUInt_native(obj, cname);
  }

  public long getColumnAsLong(String cname) {
    return getColumnAsLong_native(obj, cname);
  }

  /**
   * There is no ulong in Java, so we use long here *
   */
  public long getColumnAsULong(String cname) {
    return getColumnAsULong_native(obj, cname);
  }


  public float getColumnAsFloat(String cname) {
    return getColumnAsFloat_native(obj, cname);
  }

  public double getColumnAsDouble(String cname) {
    return getColumnAsDouble_native(obj, cname);
  }

  public String getColumnAsString(String cname) {
    return getColumnAsString_native(obj, cname);
  }

  public short getColumnAsShort(final int cnum) {
    return getColumnNumAsShort_native(obj, cnum);
  }

  public int getColumnAsUShort(final int cnum) {
    return getColumnNumAsShort_native(obj, cnum);
  }

  public int getColumnAsInt(final int cnum) {
    return getColumnNumAsInt_native(obj, cnum);
  }

  /**
   * There is no uint in Java, so we use long here *
   */
  public long getColumnAsUInt(final int cnum) {
    return getColumnNumAsUInt_native(obj, cnum);
  }

  public long getColumnAsLong(final int cnum) {
    return getColumnNumAsLong_native(obj, cnum);
  }

  /**
   * There is no ulong in Java, so we use long here *
   */
  public long getColumnAsULong(final int cnum) {
    return getColumnNumAsULong_native(obj, cnum);
  }

  public float getColumnAsFloat(final int cnum) {
    return getColumnNumAsFloat_native(obj, cnum);
  }

  public double getColumnAsDouble(final int cnum) {
    return getColumnNumAsDouble_native(obj, cnum);
  }

  public String getColumnAsString(final int cnum) {
    return getColumnNumAsString_native(obj, cnum);
  }

  public String asString() {
    return asString_native(obj);
  }

  public void dump(Writer w) {
    dump_native(obj, w);
  }

  private native short getColumnAsShort_native(long obj, String cname);
  private native int getColumnAsUShort_native(long obj, String cname);
  private native int getColumnAsInt_native(long obj, String cname);
  private native long getColumnAsUInt_native(long obj, String cname);
  private native long getColumnAsLong_native(long obj, String cname);
  private native long getColumnAsULong_native(long obj, String cname);
  private native float getColumnAsFloat_native(long obj, String cname);
  private native double getColumnAsDouble_native(long obj, String cname);
  private native String getColumnAsString_native(long obj, String cname);

  private native short getColumnNumAsShort_native(long obj, final int cnum);
  private native int getColumnNumAsUShort_native(long obj, final int cnum);
  private native int getColumnNumAsInt_native(long obj, final int cnum);
  private native long getColumnNumAsUInt_native(long obj, final int cnum);
  private native long getColumnNumAsLong_native(long obj, final int cnum);
  private native long getColumnNumAsULong_native(long obj, final int cnum);
  private native float getColumnNumAsFloat_native(long obj, final int cnum);
  private native double getColumnNumAsDouble_native(long obj, final int cnum);
  private native String getColumnNumAsString_native(long obj, final int cnum);


  private native String asString_native(long obj);
  private native void dump_native(long obj, Writer os);

  static {
    System.loadLibrary("fastbit4java-1.0");
  }
}