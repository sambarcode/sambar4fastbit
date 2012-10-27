package com.globalpark.fastbit;

public class FastbitPartition {
  protected long obj;

  public FastbitPartition(String path) {
    obj = createCppObject(path);
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

  private final native long createCppObject(String path);

  private final native void deleteCppObject(long obj);

  static {
    System.loadLibrary("fastbit4java-1.0");
  }
}