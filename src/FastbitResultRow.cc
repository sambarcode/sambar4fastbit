#include "FastbitResultRow.h" 
#include <iostream>
#include "ibis.h"
#include "macros.h"

using namespace std;
#define SETUPENV(type)                                  \
    ibis::table::cursor * cur;                          \
    const char * cname;                                 \
    cur = secure_cast<ibis::table::cursor *>(cp);       \
    cname = env->GetStringUTFChars(arg_cname, NULL);    \
    type val;                                           \


#define SETUPINTENV(type)                               \
    ibis::table::cursor * cur = secure_cast<ibis::table::cursor *>(cp);       \
    unsigned int cnum = cnum_arg;                        \
    type val;                                           \

#define CLEARENV  \
    env->ReleaseStringUTFChars(arg_cname, cname);



inline static void throwException(JNIEnv * env, const char * msg)
{
  throwNamedException(env, "com/globalpark/fastbit/exception/FastbitResultRowException", msg);
}


JNIEXPORT jshort JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsShort_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(int16_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }
  try
  {
      cur->getColumnAsShort(cname, val);
  }
  CATCH_DEFAULT("getColumnAsShort failed");
  CLEARENV;
  return (jshort)val;
}

JNIEXPORT jshort JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsShort_1native (JNIEnv * env, jobject self, jlong cp, jint cnum_arg)
{
  SETUPINTENV(int16_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }
  try
  {
      cur->getColumnAsShort(cnum, val);
  }
  CATCH_DEFAULT("getColumnNumAsShort failed");
  return (jshort)val;
}

JNIEXPORT jint JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsUShort_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(uint16_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsUShort(cname, val);
  }
  CATCH_DEFAULT("getColumnAsUShort failed");
  CLEARENV;
  return (jint) val;
}

JNIEXPORT jint JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsUShort_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(uint16_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsUShort(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsUShort failed");
  return (jint) val;
}

JNIEXPORT jint JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsInt_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(int32_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsInt(cname, val);
  }
  CATCH_DEFAULT("getColumnAsInt failed");
  CLEARENV;
  return (jint)val;
}

JNIEXPORT jint JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsInt_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(int32_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsInt(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsInt failed");
  return (jint)val;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsUInt_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(uint32_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsUInt(cname, val);
  }
  CATCH_DEFAULT("getColumnAsUInt failed");
  CLEARENV;
  return (jint)val;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsUInt_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(uint32_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsUInt(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsUInt failed");
  return (jint)val;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsLong_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(int64_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsLong(cname, val);
  }
  CATCH_DEFAULT("getColumnAsLong failed");
  CLEARENV;
  return (jlong)val;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsLong_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(int64_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsLong(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsLong failed");
  return (jlong)val;
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsULong_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(uint64_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsULong(cname, val);
  }
  CATCH_DEFAULT("getColumnAsULong failed");
  CLEARENV;
  return (jlong)val; // remark: if value is bigger than pow(2,63), the return value will be wrong!
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsULong_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(uint64_t);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsULong(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsULong failed");
  return (jlong)val; // remark: if value is bigger than pow(2,63), the return value will be wrong!
}

JNIEXPORT jfloat JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsFloat_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(float);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsFloat(cname, val);
  }
  CATCH_DEFAULT("getColumnAsFloat failed");
  CLEARENV;
  return (jfloat)val;  // this assumes that the C float type has the same range as the Java float type. No easy workaround so leave this as is
}

JNIEXPORT jfloat JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsFloat_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(float);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsFloat(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsFloat failed");
  return (jfloat)val;  // this assumes that the C float type has the same range as the Java float type. No easy workaround so leave this as is
}

JNIEXPORT jdouble JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsDouble_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{

  SETUPENV(double);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsDouble(cname, val);
  }
  CATCH_DEFAULT("getColumnAsDouble failed");
  CLEARENV;
  return (jdouble)val; // this assumes that the C double type has the same range as the Java double type. No easy workaround so leave this as is.
}

JNIEXPORT jdouble JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsDouble_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{

  SETUPINTENV(double);
  val = 0;
  if(!cur)
    {
      throwNullPointerException(env);
      return 0;
    }

  try
  {
      cur->getColumnAsDouble(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsDouble failed");
  return (jdouble)val; // this assumes that the C double type has the same range as the Java double type. No easy workaround so leave this as is.
}

JNIEXPORT jstring JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnAsString_1native(JNIEnv * env, jobject obj, jlong cp, jstring arg_cname)
{
  SETUPENV(string);
  val = "";
  if(!cur)
    {
      throwNullPointerException(env);
      return NULL;
    }

  try
  {
      cur->getColumnAsString(cname, val);
  }
  CATCH_DEFAULT("getColumnAsString failed");
  return env->NewStringUTF(val.c_str());
}

JNIEXPORT jstring JNICALL Java_com_globalpark_fastbit_FastbitResultRow_getColumnNumAsString_1native(JNIEnv * env, jobject obj, jlong cp, jint cnum_arg)
{
  SETUPINTENV(string);
  val = "";
  if(!cur)
    {
      throwNullPointerException(env);
      return NULL;
    }

  try
  {
      cur->getColumnAsString(cnum, val);
  }
  CATCH_DEFAULT("getColumnAsString failed");
  return env->NewStringUTF(val.c_str());
}

JNIEXPORT jstring JNICALL Java_com_globalpark_fastbit_FastbitResultRow_asString_1native(JNIEnv * env, jobject obj, jlong cp)
{
  ostringstream val;

  ibis::table::cursor * cur = 0;
  if(!(cur = secure_cast<ibis::table::cursor *>(cp)))
    {
      throwNullPointerException(env);
      return NULL;
    }

  try
  {
      cur->dump( val);
      return env->NewStringUTF(val.str().c_str());
  }
  CATCH_DEFAULT("asAsSstring failed");
  return NULL;
}


JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitResultRow_dump_1native(JNIEnv * env, jobject obj, jlong cp, jobject stream)
{
  ostringstream val;

  ibis::table::cursor * cur = 0;

  if(!(cur = secure_cast<ibis::table::cursor *>(cp)))
    {
      throwNullPointerException(env);
      return;
    }

  try
  {
      cur->dump( val);
      jclass jstreamClass = env->GetObjectClass(stream);
      jmethodID mid = env->GetMethodID(jstreamClass, "write", "(Ljava/lang/String;)V");
      jstring str = env->NewStringUTF(val.str().c_str());
      env->CallVoidMethod(stream, mid, str);
  }
  CATCH_DEFAULT("dump failed");
}
