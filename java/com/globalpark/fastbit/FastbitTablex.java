package com.globalpark.fastbit;

public class FastbitTablex {
  protected long obj;

  public FastbitTablex() {
    obj = createCppObject();
  }

  public void importCSV(String csvfilename, String targetdir, String metadatastring, int numRows) {
    importCSV(csvfilename, targetdir, metadatastring, ",", numRows);
  }

  public void importCSV(String csvfilename, String targetdir, String metadatastring, String delimiters, int numRows) {
    importCSV_native(obj, csvfilename, targetdir, metadatastring, delimiters, numRows);
  }

  public void appendRow(String row, String targetdir, String metadatastring, String delimiters) {
    appendRow_native(obj, row, targetdir, metadatastring, delimiters);
  }

  public void appendRow(String row, String targetdir, String metadatastring) {
    appendRow(row, targetdir, metadatastring, ",");
  }

  public void destroy() {
    if (obj != 0) {
      deleteCppObject(obj);
    }
    obj = 0;
  }

  private final native long createCppObject();

  private final native void deleteCppObject(long obj);

  private final native void importCSV_native(long obj, String cvsfilename, String targetdir, String metadatastring, String delimiters, int numRows);

  private final native void appendRow_native(long obj, String row, String targetdir, String metadatastring, String delimiters);

  static {
    System.loadLibrary("fastbit4java-1.0");
  }
}
