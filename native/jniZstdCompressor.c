/**
    4MC
    Copyright (c) 2016, Carlo Medas
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

ZSTD:
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
**/

#include "jnihelper.h"

#include <stdio.h>
#include <stdlib.h>

// LZ4 and ZSTD
#include "zstd/zstd.h"
#include "lz4/xxhash.h"


#define EXCEPTION_STRING_MAXLEN 256

static jfieldID ZstdCompressor_finish;
static jfieldID ZstdCompressor_finished;
static jfieldID ZstdCompressor_uncompressedDirectBuf;
static jfieldID ZstdCompressor_uncompressedDirectBufLen;
static jfieldID ZstdCompressor_compressedDirectBuf;
static jfieldID ZstdCompressor_directBufferSize;


JNIEXPORT void JNICALL
Java_com_hadoop_compression_fourmc_ZstdCompressor_initIDs(
	JNIEnv *env, jclass class
	)
{
  ZstdCompressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
  ZstdCompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  ZstdCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, "uncompressedDirectBuf", "Ljava/nio/ByteBuffer;");
  ZstdCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class, "uncompressedDirectBufLen", "I");
  ZstdCompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, "compressedDirectBuf", "Ljava/nio/ByteBuffer;");
  ZstdCompressor_directBufferSize = (*env)->GetFieldID(env, class, "directBufferSize", "I");

}


JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_ZstdCompressor_compressBytesDirect(
  JNIEnv *env, jobject this)
{
	size_t r;

	jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_uncompressedDirectBuf);
	unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen);

	jobject compressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_compressedDirectBuf);

	const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);

	if (uncompressed_bytes == 0 || compressed_bytes == 0) {
    	return (jint)0;
	}
  
    // Compress
	r = ZSTD_compress(compressed_bytes, 1024*1024*1024 /**enforced before in Java code**/,
			uncompressed_bytes, uncompressed_direct_buf_len, 1);

	if (!ZSTD_isError(r)) {
        (*env)->SetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen, 0);
    } else {
        char exception_msg[EXCEPTION_STRING_MAXLEN];
       	PORTABLE_SNPRINTF_START(exception_msg, EXCEPTION_STRING_MAXLEN, "%s returned: %lu", "ZSTD_compress", r);
		PORTABLE_SNPRINTF_END(exception_msg, EXCEPTION_STRING_MAXLEN);
        THROW(env, "java/lang/InternalError", exception_msg);
    }

    return (jint)r;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_ZstdCompressor_compressBytesDirectMC(
  JNIEnv *env, jobject this)
{
	size_t r;
    jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_uncompressedDirectBuf);
    unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen);

    jobject compressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_compressedDirectBuf);

    const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);

	if (uncompressed_bytes == 0 || compressed_bytes == 0) {
        return (jint)0;
    }

    // Compress
	r = ZSTD_compress(compressed_bytes, 1024*1024*1024 /**enforced before in Java code**/,
				uncompressed_bytes, uncompressed_direct_buf_len, 3);

	if (!ZSTD_isError(r)) {
        (*env)->SetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen, 0);
    } else {
        char exception_msg[EXCEPTION_STRING_MAXLEN];
       	PORTABLE_SNPRINTF_START(exception_msg, EXCEPTION_STRING_MAXLEN, "%s returned: %lu", "ZSTD_compress", r);
		PORTABLE_SNPRINTF_END(exception_msg, EXCEPTION_STRING_MAXLEN);
        THROW(env, "java/lang/InternalError", exception_msg);
    }

    return (jint)r;
}


JNIEXPORT jint JNICALL
Java_com_hadoop_compression_fourmc_ZstdCompressor_compressBytesDirectHC(
  JNIEnv *env, jobject this, jint clevel)
{
	size_t r;
    jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_uncompressedDirectBuf);
    unsigned int uncompressed_direct_buf_len = (*env)->GetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen);

    jobject compressed_direct_buf = (*env)->GetObjectField(env, this, ZstdCompressor_compressedDirectBuf);

    const char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);

	if (uncompressed_bytes == 0 || compressed_bytes == 0) {
        return (jint)0;
    }

    // Compress
	r = ZSTD_compress(compressed_bytes, 1024*1024*1024 /**enforced before in Java code**/,
				uncompressed_bytes, uncompressed_direct_buf_len, clevel);

	if (!ZSTD_isError(r)) {
        (*env)->SetIntField(env, this, ZstdCompressor_uncompressedDirectBufLen, 0);
    } else {
        char exception_msg[EXCEPTION_STRING_MAXLEN];
       	PORTABLE_SNPRINTF_START(exception_msg, EXCEPTION_STRING_MAXLEN, "%s returned: %lu", "ZSTD_compress", r);
		PORTABLE_SNPRINTF_END(exception_msg, EXCEPTION_STRING_MAXLEN);
        THROW(env, "java/lang/InternalError", exception_msg);
	}

    return (jint)r;
}


JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_ZstdCompressor_compressBound
  (JNIEnv *env, jclass cls, jint forSize) {
    return ZSTD_compressBound(forSize);
}



JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_ZstdCompressor_xxhash32
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


