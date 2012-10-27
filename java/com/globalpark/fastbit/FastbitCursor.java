package com.globalpark.fastbit;

public class FastbitCursor {
  private long obj;
  FastbitResultRow row;

  public FastbitCursor(long table) {
    obj = getCursor_native(table);
    row = new FastbitResultRow();
    row.setCursor(obj);
  }

  private native long getCursor_native(long table);

  public void remove() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Operation is not supported");
  }

  public FastbitResultRow next() {
    next_native(obj);
    return row;
  }

  public boolean hasNext() {
    return hasNext_native(obj);
  }

  public void destroy() {
    if (obj != 0) {
      deleteCppObject(obj);
    }
    obj = 0;
  }


  protected void finalize() {
    destroy();
  }

  private native void next_native(long obj);

  private native boolean hasNext_native(long obj);

  private final native void deleteCppObject(long obj);

  static {
    System.loadLibrary("fastbit4java-1.0");
  }

}
