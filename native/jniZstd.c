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
#include <jni.h>
#include <zstd.h>
#include <error_private.h>



/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    isError
 * Signature: (J)I
 */
JNIEXPORT jboolean JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_isError
  (JNIEnv *env, jclass obj, jlong code) {
    return ZSTD_isError((size_t) code) != 0;
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    getErrorName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_getErrorName
  (JNIEnv *env, jclass obj, jlong code) {
    const char *msg = ZSTD_getErrorName(code);
    return (*env)->NewStringUTF(env, msg);
}


/*===== Streaming Compression/Decompression Part =====*/


/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    cStreamInSize
 * Signature: ()J
 */
JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_cStreamInSize
  (JNIEnv *env, jclass obj) {
    return (jint) ZSTD_CStreamInSize();
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    cStreamOutSize
 * Signature: ()J
 */
JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_cStreamOutSize
  (JNIEnv *env, jclass obj) {
    return (jint) ZSTD_CStreamOutSize();
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    dStreamInSize
 * Signature: ()J
 */
JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_dStreamInSize
  (JNIEnv *env, jclass obj) {
    return (jint) ZSTD_DStreamInSize();
}

/*
 * Class:     com_hadoop_compression_fourmc_zstd_Zstd
 * Method:    dStreamOutSize
 * Signature: ()J
 */
JNIEXPORT jint JNICALL Java_com_hadoop_compression_fourmc_zstd_Zstd_dStreamOutSize
  (JNIEnv *env, jclass obj) {
    return (jint) ZSTD_DStreamOutSize();
}
