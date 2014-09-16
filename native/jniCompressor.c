/**
    4MC
    Copyright (c) 2014, Carlo Medas
    BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice, this
      list of conditions and the following disclaimer in the documentation and/or
      other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact 4MC author at :
      - 4MC source repository : https://github.com/carlomedas/4mc

  LZ4 - Copyright (C) 2011-2014, Yann Collet - BSD 2-Clause License.
  You can contact LZ4 lib author at :
      - LZ4 source repository : http://code.google.com/p/lz4/
**/

#include "jnihelper.h"

#include <stdio.h>
#include <stdlib.h>

// LZ4
#include "lz4/lz4.h"
#include "lz4/lz4hc.h"
#include "lz4/lz4mc.h"
#include "lz4/xxhash.h"


static jfieldID Lz4Compressor_finish;
static jfieldID Lz4Compressor_finished;
static jfieldID Lz4Compressor_uncompressedDirectBuf;
static jfieldID Lz4Compressor_uncompressedDirectBufLen;
static jfieldID Lz4Compressor_compressedDirectBuf;
static jfieldID Lz4Compressor_directBufferSize;


JNIEXPORT void JNICALL
Java_com_hadoop_compression_fourmc_Lz4Compressor_initIDs(
	JNIEnv *env, jclass class
	)
{
  Lz4Compressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
  Lz4Compressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  Lz4Compressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, "uncompressedDirectBuf", "Ljava/nio/ByteBuffer;");
  Lz4Compressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class, "uncompressedDirectBufLen", "I");
  Lz4Compressor_compressedDirectBuf = (*env)->GetFieldID(env, class, "compressedDirectBuf", "Ljava/nio/ByteBuffer;");
  Lz4Compressor_directBufferSize = (*env)->GetFieldID(env, class, "directBufferSize", "I");

}


JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_Lz4Compressor_compressBytesDirect(
  JNIEnv *env, jobject this)
{

	jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_uncompressedDirectBuf);
	unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen);

	jobject compressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_compressedDirectBuf);

	const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    if (uncompressed_bytes == 0) {
    	return (jint)0;
	}

	char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
    if (compressed_bytes == 0) {
		return (jint)0;
	}
  
    // Compress
    int r = LZ4_compress(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len);

    if (r > 0) {
        (*env)->SetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen, 0);
    } else {
        const int msg_len = 32;
        char exception_msg[msg_len];
        snprintf(exception_msg, msg_len, "%s returned: %d", "LZ4_compress_limitedOutput", r);
        THROW(env, "java/lang/InternalError", exception_msg);
    }

    return (jint)r;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_Lz4Compressor_compressBytesDirectMC(
  JNIEnv *env, jobject this)
{
    jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_uncompressedDirectBuf);
    unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen);

    jobject compressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_compressedDirectBuf);

    const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    if (uncompressed_bytes == 0) {
        return (jint)0;
    }

    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
    if (compressed_bytes == 0) {
        return (jint)0;
    }

    // Compress
    int r = LZ4_compressMC(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len);

    if (r > 0) {
        (*env)->SetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen, 0);
    } else {
        const int msg_len = 32;
        char exception_msg[msg_len];
        snprintf(exception_msg, msg_len, "%s returned: %d", "LZ4_compress_limitedOutput", r);
        THROW(env, "java/lang/InternalError", exception_msg);
    }

    return (jint)r;
}


JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_Lz4Compressor_compressBytesDirectHC(
  JNIEnv *env, jobject this, jint clevel)
{
    jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_uncompressedDirectBuf);
    unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen);

    jobject compressed_direct_buf = (*env)->GetObjectField(env, this, Lz4Compressor_compressedDirectBuf);

    const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    if (uncompressed_bytes == 0) {
        return (jint)0;
    }

    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
    if (compressed_bytes == 0) {
        return (jint)0;
    }

    // Compress
    int r = LZ4_compressHC2(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len, clevel);

    if (r > 0) {
        (*env)->SetIntField(env, this, Lz4Compressor_uncompressedDirectBufLen, 0);
    } else {
        const int msg_len = 32;
        char exception_msg[msg_len];
        snprintf(exception_msg, msg_len, "%s returned: %d", "LZ4_compress_limitedOutput", r);
        THROW(env, "java/lang/InternalError", exception_msg);
    }

    return (jint)r;
}


JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_Lz4Compressor_compressBound
  (JNIEnv *env, jclass cls, jint forSize) {
    return LZ4_compressBound(forSize);
}



JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_Lz4Compressor_xxhash32
  (JNIEnv *env, jclass cls, jbyteArray buf, jint off, jint len, jint seed) {

  char* in;
  jint h32;

  in = (char*) (*env)->GetPrimitiveArrayCritical(env, buf, 0);
  if (in == NULL) {
      return (jint)0;
  }

  h32 = XXH32(in + off, len, seed);

  (*env)->ReleasePrimitiveArrayCritical(env, buf, in, 0);

  return h32;
}


