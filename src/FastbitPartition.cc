#include "FastbitPartition.h" 
#include <iostream>
#include "ibis.h"
#include "macros.h"
using namespace std;

inline static void throwException(JNIEnv * env, const char * msg)
{
    throwNamedException(env, "com/globalpark/fastbit/exception/FastbitPartitionException", msg);
}


JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitPartition_createCppObject(JNIEnv * env, jobject obj, jstring path)
{
  const char * datadir = 0;
  try
  {
      datadir = env->GetStringUTFChars(path, NULL);
      
      ibis::part * partition = new ibis::part(datadir,true);
      env->ReleaseStringUTFChars(path, datadir);

      return reinterpret_cast<jlong>(partition);
  }
  CATCH_DEFAULT("createCppObject failed");
  env->ReleaseStringUTFChars(path, datadir);
  return 0;
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitPartition_deleteCppObject(JNIEnv * env, jobject obj, jlong p)
{

  ibis::part * partition = 0;
  if(!(partition = secure_cast<ibis::part *>(p)))
  {
      throwNullPointerException(env);
      return;
  }
  delete partition;
}
