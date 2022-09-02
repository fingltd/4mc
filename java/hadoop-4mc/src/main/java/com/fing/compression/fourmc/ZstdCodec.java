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

 * ZSTD License:
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the native directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
**/
package com.fing.compression.fourmc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link CompressionCodec} for a streaming
 * <b>ZSTD</b> compression/decompression pair.
 */
public class ZstdCodec implements Configurable, CompressionCodec {
    private static final Log LOG = LogFactory.getLog(ZstdCodec.class.getName());

    public static final int ZSTD_BUFFER_SIZE = 4 * 1024 * 1024;

    private Configuration conf;


    public void setConf(Configuration conf) {
        this.conf = conf;
    }


    public Configuration getConf() {
        return conf;
    }

    private static boolean nativeLoaded = false;

    static {
        if (FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            nativeLoaded = ZstdCompressor.isNativeLoaded() &&
                    ZstdDecompressor.isNativeLoaded();

            if (nativeLoaded) {
                LOG.info("Successfully loaded & initialized native-4mc library");
            } else {
                LOG.error("Failed to load/initialize native-4mc library");
            }
        } else {
            LOG.error("Cannot load native-4mc without native-hadoop");
        }
    }

    public static boolean isNativeLoaded(Configuration conf) {
        //assert conf != null : "Configuration cannot be null!";
        return nativeLoaded;
    }

    
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return createOutputStream(out, createCompressor());
    }

    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor) throws IOException {

        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        int bufferPlusOverhead = ZstdCompressor.compressBound(ZSTD_BUFFER_SIZE);
        return new BlockCompressorStream(out, compressor, ZSTD_BUFFER_SIZE, bufferPlusOverhead - ZSTD_BUFFER_SIZE);
    }


    public Class<? extends Compressor> getCompressorType() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdCompressor.class;
    }


    public Compressor createCompressor() {
        assert conf != null : "Configuration cannot be null! You must call setConf() before creating a compressor.";
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new ZstdCompressor(ZSTD_BUFFER_SIZE);
    }


    public CompressionInputStream createInputStream(InputStream in)
            throws IOException {
        return createInputStream(in, createDecompressor());
    }


    public CompressionInputStream createInputStream(InputStream in,
                                                    Decompressor decompressor)
            throws IOException {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new BlockDecompressorStream(in, decompressor, ZSTD_BUFFER_SIZE);
    }


    public Class<? extends Decompressor> getDecompressorType() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdDecompressor.class;
    }


    public Decompressor createDecompressor() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new ZstdDecompressor(ZSTD_BUFFER_SIZE);
    }

    /**
     * Get the default filename extension for this kind of compression.
     *
     * @return the extension including the '.'
     */
    public String getDefaultExtension() {
        return ".zstd_fast";
    }

}