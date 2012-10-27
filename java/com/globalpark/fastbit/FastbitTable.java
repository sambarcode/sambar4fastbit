package com.globalpark.fastbit;

import com.globalpark.fastbit.exception.FastbitTableException;

import java.io.Writer;

public class FastbitTable {
  protected long obj;
  private boolean isSource;
  private String tableDir;
  
  public static void setConf(final String fileName) {
  	loadConf(fileName);
  }

  public FastbitTable(FastbitPartition p) {
    isSource = true;
    obj = createCppObject(p);
  }

  public FastbitTable(final String dir) {
    isSource = true;
    obj = createTable(dir);
    tableDir = dir;
  }

  public FastbitTable(long p) {
    isSource = false;
    obj = p;
  }

  public void buildIndex(final String colName, final String option) {
    buildIndex_native(obj, colName, option);
  }

  public void buildIndexes(final String option) {
    buildIndexes_native(obj, option);
  }

  public FastbitTable select(String selects, String wheres) {
    return new FastbitTable(select_native(obj, selects, wheres));
  }

  public FastbitTable groupby(String groupbys) {
    if (isSource) {
      throw new FastbitTableException("Groupby not allowed on source tables");
    }
    return new FastbitTable(groupby_native(obj, groupbys));
  }

  public void orderby(String orderbys) {
    if (isSource) {
      throw new FastbitTableException("Orderby not allowed on source tables");
    }
    orderby_native(obj, orderbys);
  }

  public long nrows() {
    return nrows_native(obj);
  }

  public long ncolumns() {
    return ncolumns_native(obj);
  }

  public String[] columnnames() {
    return columnnames_native(obj);
  }

  public long[] columntypes() {
    return columntypes_native(obj);
  }

  public String asString() {
    return asString_native(obj);
  }

  public void dump(Writer w) {
    dump_native(obj, w);
  }

  public FastbitCursor getCursor() {
    return new FastbitCursor(obj);
  }

  public void destroy(final boolean flush) {
    if (obj != 0) {
      deleteCppObject(obj); 
    }
    obj = 0;
    if (flush && tableDir != null) {
    	flushFromFileManager(tableDir);
    }
    tableDir = null;
  }

  private final native long createTable(final String dir);

  private final native long createCppObject(FastbitPartition p);

  private final native void buildIndex_native(final long obj, final String colName, final String option);

  private final native void buildIndexes_native(final long obj, final String option);

  private final native long select_native(long obj, String selects, String wheres);

  private final native long groupby_native(long obj, String groupbys);

  private final native void orderby_native(long obj, String orderbys);

  private final native long nrows_native(long obj);

  private final native long ncolumns_native(long obj);

  private final native String[] columnnames_native(long obj);

  private final native long[] columntypes_native(long obj);

  private native String asString_native(long obj);

  private native void dump_native(long obj, Writer os);

  private final native void deleteCppObject(long obj);
  
  private final native void flushFromFileManager(final String dir);
  
  private static native void loadConf(final String fileName);

  public static final native long typeUNKNOWN();

  public static final native long typeOID();

  public static final native long typeBYTE();

  public static final native long typeUBYTE();

  public static final native long typeSHORT();

  public static final native long typeUSHORT();

  public static final native long typeINT();

  public static final native long typeUINT();

  public static final native long typeLONG();

  public static final native long typeULONG();

  public static final native long typeFLOAT();

  public static final native long typeDOUBLE();

  public static final native long typeCATEGORY();

  public static final native long typeTEXT();

  public static final native long typeBLOB();

  static {
    System.loadLibrary("fastbit4java-1.0");
  }
}