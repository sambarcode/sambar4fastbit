#include "FastbitTable.h" 
#include <iostream>
#include "ibis.h"
#include "macros.h"


using namespace std;

inline static void throwException(JNIEnv * env, const char * msg)
{
    throwNamedException(env, "com/globalpark/fastbit/exception/FastbitTableException", msg);
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_createTable(JNIEnv * env, jobject self, jstring dir) {
  try {
      const char * dir_name = env->GetStringUTFChars(dir, NULL);
      if(!dir_name)
      {
         throw "Getting table dir name failed";
      }
      ibis::table * table = ibis::table::create(dir_name);
      env->ReleaseStringUTFChars(dir, dir_name);
      return reinterpret_cast<jlong>(table);
      }
   CATCH_DEFAULT("createTable failed");
   return 0;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_createCppObject(JNIEnv * env, jobject self, jobject partition)
{
    try 
    {
        ibis::table * table = 0;
        jclass p = env->GetObjectClass(partition);
        
        jfieldID fid = env->GetFieldID(p,"obj", "J");
        
        jlong ptr = env->GetLongField(partition, fid);
        ibis::part * part = 0;
        
        if(!( part = secure_cast<ibis::part *>(ptr)))
        {
            throwNullPointerException(env);
            return 0;
        }

        table = ibis::table::create(*part);
        
        return reinterpret_cast<jlong>(table); 
    }
    CATCH_DEFAULT("Failed to read partition");
    return 0;
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_buildIndexes_1native(JNIEnv * env, jobject self, jlong tp, jstring arg_option) {
    const char * option = env->GetStringUTFChars(arg_option, NULL);
    ibis::table * table = 0;
    if(!(table = secure_cast<ibis::table *>(tp)))
    {
       throwNullPointerException(env);
       return;
    }
    table->buildIndexes(option);
    table->indexSpec(option, 0);
    env->ReleaseStringUTFChars(arg_option, option);
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_buildIndex_1native(JNIEnv * env, jobject self, jlong tp, jstring arg_col, jstring arg_option) {
     const char * option = env->GetStringUTFChars(arg_option, NULL);
     const char * col = env->GetStringUTFChars(arg_col, NULL);
     ibis::table * table = 0;
     if(!(table = secure_cast<ibis::table *>(tp)))
        {
           throwNullPointerException(env);
           return;
        }
     table->buildIndex(col, option);
     table->indexSpec(option, col);
     env->ReleaseStringUTFChars(arg_option, option);
     env->ReleaseStringUTFChars(arg_col, col);
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_deleteCppObject(JNIEnv * env, jobject obj, jlong tp)
{
    ibis::table * table = 0;
    if(!(table = secure_cast<ibis::table *>(tp)))
    {
        throwNullPointerException(env);
        return;
    }
    delete table;
    // TODO: unlaod the dir from the Fastbit filemanager cache
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_flushFromFileManager(JNIEnv * env, jobject obj, jstring arg_dir) {
  const char * dir_name = env->GetStringUTFChars(arg_dir, NULL);
  ibis::fileManager::instance().flushDir(dir_name);
  env->ReleaseStringUTFChars(arg_dir, dir_name);
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_loadConf(JNIEnv * env, jclass cls, jstring arg_path) {
  const char * conf_path = env->GetStringUTFChars(arg_path, NULL);
  ibis::gParameters().read(conf_path);
  env->ReleaseStringUTFChars(arg_path, conf_path);
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_select_1native(JNIEnv * env, jobject obj, jlong tp, jstring arg_selects, jstring arg_wheres)
{
    const char * selects = 0;
    const char * wheres = 0;
    ibis::table * select = 0;

    ibis::table * table = 0;

    if(!(table = secure_cast<ibis::table *>(tp)))
    {
        throwNullPointerException(env);
        return 0;
    }

    selects = env->GetStringUTFChars(arg_selects, NULL);
    wheres = env->GetStringUTFChars(arg_wheres, NULL);

    try 
    {
        select = table->select(selects, wheres);
        if(!select)
        {
            throw "Select failed";
        }

    }
    CATCH_DEFAULT("Select failed");
    
    env->ReleaseStringUTFChars(arg_selects, selects);
    env->ReleaseStringUTFChars(arg_wheres, wheres);
    return reinterpret_cast<jlong>(select); 
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_groupby_1native(JNIEnv * env, jobject obj, jlong tp, jstring arg_groupbys)
{
    const char * groupbys = 0;
    ibis::table* groupby = 0;

    ibis::table * table = 0;

    if(!(table = secure_cast<ibis::table *>(tp)))
    {
        throwNullPointerException(env);
        return 0;
    }

    groupbys = env->GetStringUTFChars(arg_groupbys, NULL);
    try 
    {
        groupby = table->groupby(groupbys);
        if(!groupby) 
        {
            throw "Group by failed";
        }

    }
    CATCH_DEFAULT("Group by failed");

    env->ReleaseStringUTFChars(arg_groupbys, groupbys);  
    return reinterpret_cast<jlong>(groupby); 
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_orderby_1native(JNIEnv * env, jobject obj, jlong tp, jstring arg_orderbys)
{
    const char * orderbys = 0;
    ibis::table * table = 0;

    if(!(table = secure_cast<ibis::table *>(tp)))
    {
        throwNullPointerException(env);
        return;
    }

    orderbys = env->GetStringUTFChars(arg_orderbys, NULL);
    try
    {
        table->orderby(orderbys);
    }
    CATCH_DEFAULT("Order by failed");
    env->ReleaseStringUTFChars(arg_orderbys, orderbys);  
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_nrows_1native(JNIEnv * env, jobject obj, jlong tp)
{
    try
    {
        ibis::table * table = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return 0;
        }
        return table->nRows();
    }
    CATCH_DEFAULT("nrows failed");
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_ncolumns_1native(JNIEnv * env, jobject obj, jlong tp)
{
    try
    {
        ibis::table * table = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return 0;
        }
        return table->nColumns();
    }
    CATCH_DEFAULT("ncolumns failed")
    return 0;
}

JNIEXPORT jobjectArray JNICALL Java_com_globalpark_fastbit_FastbitTable_columnnames_1native(JNIEnv * env, jobject obj, jlong tp)
{

    try 
    {
        ibis::table * table = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return NULL;
        }
        ibis::table::stringList nms = table->columnNames();

        jclass stringClass = env->FindClass("java/lang/String");

        jobjectArray result = env->NewObjectArray((jsize)nms.size(), stringClass, NULL);

#if FASTBIT_IBIS_INT_VERSION >= 1010900
        ibis::array_t<const char*>::iterator it;
#else
        vector<const char*>::iterator it;
#endif
        it = nms.begin();
        jsize i = 0;
        while (it != nms.end())
        {
            jstring s = env->NewStringUTF((const char*)*it);
            env->SetObjectArrayElement(result, i, s);
            ++it;
            ++i;
        }
        return result;
    }
    CATCH_DEFAULT("columnnames failed");
    return NULL;
}

JNIEXPORT jlongArray JNICALL Java_com_globalpark_fastbit_FastbitTable_columntypes_1native(JNIEnv * env, jobject obj, jlong tp)
{
    try 
    {
        ibis::table * table = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return NULL;
        }
        ibis::table::typeList tps = table->columnTypes();
        jlongArray result = env->NewLongArray((jsize)tps.size());

#if FASTBIT_IBIS_INT_VERSION >= 1010900
        ibis::array_t<ibis::TYPE_T>::iterator t;
#else
        vector<ibis::TYPE_T>::iterator t;
#endif
        t = tps.begin();
        jsize i = 0;
        while (t != tps.end())
        {
            const jlong l = *t;
            env->SetLongArrayRegion(result, i, 1,(const jlong*)&l);
            ++t;
            ++i;
        }
        return result;
    }
    CATCH_DEFAULT("columntypes failed");
    return NULL;
}

JNIEXPORT jstring JNICALL Java_com_globalpark_fastbit_FastbitTable_asString_1native(JNIEnv * env, jobject obj, jlong tp)
{

    try
    {
        ostringstream val;
        ibis::table * table  = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return NULL;
        }

        table->dump( val);
        return env->NewStringUTF(val.str().c_str());    
    }
    CATCH_DEFAULT("asString failed");
    return NULL;
}


JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTable_dump_1native(JNIEnv * env, jobject obj, jlong tp, jobject stream)
{
    try
    {
        ostringstream val;
        ibis::table * table = 0;
        if(!(table = secure_cast<ibis::table *>(tp)))
        {
            throwNullPointerException(env);
            return;
        }
        
        table->dump( val);
        jclass jstreamClass = env->GetObjectClass(stream);
        if(!jstreamClass)
        {
            throw "Invalid stream";
            return;
        }
        jmethodID mid = env->GetMethodID(jstreamClass, "write", "(Ljava/lang/String;)V");

        if(!mid) // Stream has no write method
        {
            throw "Invalid stream";
            return;
        }
       
        jstring str = env->NewStringUTF(val.str().c_str());
        env->CallVoidMethod(stream, mid, str);
    }
    CATCH_DEFAULT("dump failed");
}


/*
  Functions as replacement for constants
*/

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeUNKNOWN(JNIEnv * env, jclass cls)
{
  return ibis::UNKNOWN_TYPE;
}


JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeOID(JNIEnv * env, jclass cls)
{
  return ibis::OID;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeBYTE(JNIEnv * env, jclass cls)
{
  return ibis::BYTE;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeUBYTE(JNIEnv * env, jclass cls)
{
  return ibis::UBYTE;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeSHORT(JNIEnv * env, jclass cls)
{
  return ibis::SHORT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeUSHORT(JNIEnv * env, jclass cls)
{
  return ibis::USHORT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeINT(JNIEnv * env, jclass cls)
{
  return ibis::INT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeUINT(JNIEnv * env, jclass cls)
{
  return ibis::UINT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeLONG(JNIEnv * env, jclass cls)
{
  return ibis::LONG;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeULONG(JNIEnv * env, jclass cls)
{
  return ibis::ULONG;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeFLOAT(JNIEnv * env, jclass cls)
{
  return ibis::FLOAT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeDOUBLE(JNIEnv * env, jclass cls)
{
  return ibis::DOUBLE;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeCATEGORY(JNIEnv * env, jclass cls)
{
  return ibis::CATEGORY;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeTEXT(JNIEnv * env, jclass cls)
{
  return ibis::TEXT;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTable_typeBLOB(JNIEnv * env, jclass cls)
{
  return ibis::BLOB;
}
