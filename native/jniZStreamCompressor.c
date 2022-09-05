/**
    4MC
    Copyright (c) 2016, Xianjin YE
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
#include <zstd.h>
#include <error_private.h>

/* field IDs will not change in the same vm */
static jfieldID src_pos_id;
static jfieldID dst_pos_id;
static jfieldID o_buff_len_id;

/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    initIDs
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_initIDs
  (JNIEnv *env, jclass obj) {
    src_pos_id = (*env)->GetFieldID(env, obj, "srcPos", "J");
    dst_pos_id = (*env)->GetFieldID(env, obj, "dstPos", "J");
    o_buff_len_id = (*env)->GetFieldID(env, obj, "oBuffLen", "I");
}


/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    createCStream
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_createCStream
  (JNIEnv *env, jclass obj) {
    return (jlong) ZSTD_createCStream();
}


/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    freeCStream
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_freeCStream
  (JNIEnv *env, jclass obj, jlong stream) {
    return (jint) ZSTD_freeCStream((ZSTD_CStream *) stream);
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    initCStream
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_initCStream
  (JNIEnv *env, jclass obj, jlong stream, jint level) {
    return (jint) ZSTD_initCStream((ZSTD_CStream *) stream, level);
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    compressStream
 * Signature: (JLjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_compressStream
  (JNIEnv *env, jobject this, jlong stream, jobject dst, jint dst_size, jobject src, jint src_size) {

    size_t size = (size_t)(0 - ZSTD_error_memory_allocation);

    size_t src_pos = (size_t)(*env)->GetLongField(env, this, src_pos_id);

    void *dst_buff = (*env)->GetDirectBufferAddress(env, dst);
    if (dst_buff == NULL) return (jint) size;
    void *src_buff = (*env)->GetDirectBufferAddress(env, src);
    if (src_buff == NULL) return (jint) size;


    ZSTD_outBuffer output = { dst_buff, dst_size, 0 };
    ZSTD_inBuffer input = { src_buff, src_size, src_pos };
    size = ZSTD_compressStream((ZSTD_CStream *) stream, &output, &input);
    (*env)->SetLongField(env, this, src_pos_id, input.pos);
    (*env)->SetLongField(env, this, dst_pos_id, output.pos);
    (*env)->SetIntField(env, this, o_buff_len_id, output.pos);
    return (jint) size;
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_ZstdStreamCompressor
 * Method:    endStream
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_fing_compression_fourmc_zstd_ZstdStreamCompressor_endStream
  (JNIEnv *env, jobject this, jlong stream, jobject dst, jint dst_offset, jint dst_size) {

    size_t size = (size_t)(0 - ZSTD_error_memory_allocation);

    void *dst_buff = (*env)->GetDirectBufferAddress(env, dst);
    if (dst_buff != NULL) {
      ZSTD_outBuffer output = { ((char *)dst_buff) + dst_offset, dst_size, 0 };
      size = ZSTD_endStream((ZSTD_CStream *) stream, &output);
      size_t o_buff_len = dst_offset + output.pos;
      (*env)->SetLongField(env, this, dst_pos_id, output.pos);
      (*env)->SetIntField(env, this, o_buff_len_id, o_buff_len);
    }
    return (jint) size;
}
