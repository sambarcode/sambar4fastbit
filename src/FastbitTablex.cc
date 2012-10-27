#include "FastbitTablex.h" 
#include <iostream>
#include "ibis.h"
#include "macros.h"

using namespace std;

inline static void throwException(JNIEnv * env, const char * msg)
{
    throwNamedException(env, "com/globalpark/fastbit/exception/FastbitTablexException", msg);
}

JNIEXPORT jlong JNICALL Java_com_globalpark_fastbit_FastbitTablex_createCppObject(JNIEnv * env, jobject obj)
{
    try
    {
        ibis::tablex * table;
        table = ibis::tablex::create();
        return reinterpret_cast<jlong>(table); 
    }
    CATCH_DEFAULT("createCppObject failed");
    return 0;
}
JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTablex_deleteCppObject(JNIEnv * env, jobject obj, jlong p)
{
    ibis::tablex * table = 0;
    if(!(table = secure_cast<ibis::tablex *>(p)))
    {
        throwNullPointerException(env);
        return;
    }
    delete table;
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTablex_importCSV_1native(JNIEnv * env, jobject obj, jlong p, jstring csvfilename_arg, 
                                                                                   jstring targetdir_arg, jstring metadatastring_arg, jstring delimiters_arg, jint num_rows)
{
    ibis::tablex * table = 0;
    const char * metadatastring = 0;
    const char * targetdir = 0;
    const char * csvfilename = 0;
    const char * delimiters = 0;
    const char * tablename = 0;
    const char * tabledesc = 0;
    const char * idx = 0;
//    int nrpf = 0;
    int err;

    if (!(table = secure_cast<ibis::tablex *>(p)))
    {
        throwException(env, "Invalid pointer");
        return; 
    }
 
    try
    {
        metadatastring = env->GetStringUTFChars(metadatastring_arg, NULL);
        csvfilename = env->GetStringUTFChars(csvfilename_arg, NULL);
        targetdir = env->GetStringUTFChars(targetdir_arg, NULL);
        delimiters = env->GetStringUTFChars(delimiters_arg, NULL); 
        table->parseNamesAndTypes(metadatastring);
        err = table->readCSV(csvfilename, num_rows, targetdir, delimiters);
        if(err < 0)
            throwException(env, "failed to parse csv file");
        err = table->write(targetdir, tablename, tabledesc, idx);
        if(err < 0)
            throwException(env, "Failed to write csv file");
        table->clearData();
    }
    CATCH_DEFAULT("csv import failed");
    env->ReleaseStringUTFChars(csvfilename_arg, csvfilename);
    env->ReleaseStringUTFChars(targetdir_arg, targetdir);
    env->ReleaseStringUTFChars(metadatastring_arg, metadatastring);
    env->ReleaseStringUTFChars(delimiters_arg, delimiters);
}

JNIEXPORT void JNICALL Java_com_globalpark_fastbit_FastbitTablex_appendRow_1native(JNIEnv * env, jobject obj, jlong p, jstring row_arg, 
                                                                                   jstring targetdir_arg, jstring metadatastring_arg,jstring delimiters_arg)
{
    ibis::tablex * table = 0;
    const char * metadatastring = 0;
    const char * targetdir = 0;
    const char * row = 0;
    const char * delimiters = 0;
    const char * tablename = 0;
    const char * tabledesc = 0;
    const char * idx = 0;
    int nrpf = 0;
    int err;

    try
    {
        if (!(table = secure_cast<ibis::tablex *>(p)))
        {
            throwNullPointerException(env);
            return;
        }
        metadatastring = env->GetStringUTFChars(metadatastring_arg, NULL);
        row = env->GetStringUTFChars(row_arg, NULL);
        targetdir = env->GetStringUTFChars(targetdir_arg, NULL);
        delimiters = env->GetStringUTFChars(delimiters_arg, NULL); 
        table->parseNamesAndTypes(metadatastring);
        err = table->appendRow(row, delimiters);
        if(err < 0)
            throwException(env, "failed to append Row");
        err = table->write(targetdir, tablename, tabledesc, idx);
        if(err < 0)
            throwException(env, "Failed to write csv file");
        table->clearData();
    }
    CATCH_DEFAULT("appendRow failed");
    env->ReleaseStringUTFChars(row_arg, row);
    env->ReleaseStringUTFChars(targetdir_arg, targetdir);
    env->ReleaseStringUTFChars(metadatastring_arg, metadatastring);
    env->ReleaseStringUTFChars(delimiters_arg, delimiters);
}
