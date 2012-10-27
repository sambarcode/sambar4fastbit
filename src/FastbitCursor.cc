#include "FastbitCursor.h" 
#include <iostream>
#include "ibis.h"
#include "macros.h"

using namespace std;

inline static void throwException(JNIEnv * env, const char * msg)
{
    throwNamedException(env, "com/globalpark/fastbit/exception/FastbitCursorException", msg);
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitCursor_deleteCppObject(JNIEnv * env, jobject obj, jlong cp)
{
    ibis::table::cursor * cur = secure_cast<ibis::table::cursor *>(cp);
    if(!cur)
    {
        throwNullPointerException(env);
        return;
    }
    delete cur;
}


JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitCursor_getCursor_1native(JNIEnv * env, jobject obj, jlong tp)
{
    ibis::table::cursor * cur = 0;

    ibis::table * table = secure_cast<ibis::table *>(tp);

    if(!table)
    {
        throwNullPointerException(env);
        return 0;
    }

    try {
        cur = table->createCursor();
        return reinterpret_cast<jlong>(cur);
    }
    CATCH_DEFAULT("Create cursor failed");

    if(!cur)
    {
        throwException(env, "Create cursor failed");
    }
    return 0;
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitCursor_next_1native(JNIEnv * env, jobject obj, jlong cp)
{
    ibis::table::cursor * cur = secure_cast<ibis::table::cursor *>(cp);

    if(!cur)
    {
        throwNullPointerException(env);
        return;
    }
    try {
        if(cur->fetch() == -1)
        {
            throwException(env, "Fastbit Row out of Range");
        }
    }
    CATCH_DEFAULT("next failed");
}

JNIEXPORT jboolean JNICALL Java_com_globalpark_fastbit_FastbitCursor_hasNext_1native(JNIEnv * env, jobject obj, jlong cp)
{
    ibis::table::cursor * cur = secure_cast<ibis::table::cursor *>(cp);

    if(!cur)
    {
        throwNullPointerException(env);
        return false;
    }

    try
    {
        if(cur->nRows() == 0) return false;
        if((int)cur->getCurrentRowNumber() == -1) return true;
        return (jboolean) (cur->getCurrentRowNumber() < (cur->nRows() - 1));
    }
    CATCH_DEFAULT("hasNext failed");
    return false;
}
