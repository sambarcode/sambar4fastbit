#ifndef _MACROS_H_
#define _MACROS_H_
#include <typeinfo>
#include <exception>

using namespace std;

#define CATCH_DEFAULT(msg)                               \
  catch(const char * errstr)                             \
  {                                                      \
      throwException(env, errstr);                       \
  }                                                      \
  catch(std::exception&  err)                            \
  {                                                      \
      throwException(env, err.what());                   \
  }                                                      \
  catch(...)                                             \
  {                                                      \
      throwException(env, msg);                          \
  }                                                      \
  
inline static void throwNullPointerException(JNIEnv * env)
{
    jclass newExcCls;
    newExcCls = env->FindClass("java/lang/NullPointerException");
    env->ThrowNew(newExcCls, "JNI: passed pointer invalid");
    env->DeleteLocalRef(newExcCls);
}

template <typename T> T secure_cast(jlong p)
{
    T ret;
    if (!p)
    {
        return NULL;
    }
    ret = reinterpret_cast<T>(p);
    if (typeid(ret)!=typeid(T))
    {
        return NULL;
    }

    return ret;
}

inline void throwNamedException(JNIEnv * env, const char * classpath, const char * msg)
{
    jclass newExcCls;
    newExcCls = env->FindClass(classpath);
    if(!newExcCls)
    {
        newExcCls = env->FindClass("java/lang/RuntimeException");
    }
    env->ThrowNew(newExcCls, msg);      
}

#endif
