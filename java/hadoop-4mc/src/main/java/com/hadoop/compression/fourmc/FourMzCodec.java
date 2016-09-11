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
package com.hadoop.compression.fourmc;

import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Same as FourMcCoded but handling 4mz (ZSTD version).
 */
public class FourMzCodec extends ZstdCodec {

    public static final int FOURMZ_MAGIC = 0x344D5A00;
    public static final int FOURMZ_VERSION = 1;
    public static final int FOURMC_MAX_BLOCK_SIZE = 4 * 1024 * 1024;
    public static final String FOURMZ_DEFAULT_EXTENSION = ".4mz";
    public static final String FOURMZ_BLOCK_SIZE_KEY = "io.compression.codec.4mz.blocksize";

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return createOutputStream(out, createCompressor());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new FourMzOutputStream(out, compressor, FOURMC_MAX_BLOCK_SIZE);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new FourMzInputStream(in, decompressor, FOURMC_MAX_BLOCK_SIZE);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in) throws IOException {
        return createInputStream(in, createDecompressor());
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor() {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new ZstdDecompressor(FOURMC_MAX_BLOCK_SIZE);
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdCompressor.class;
    }

    @Override
    public Compressor createCompressor() {
        assert getConf() != null : "Configuration cannot be null! You must call setConf() before creating a compressor.";
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new ZstdCompressor(getCompressionBlockSize());
    }


    @Override
    public String getDefaultExtension() {
        return FOURMZ_DEFAULT_EXTENSION;
    }

    /**
     * 4MB has been decided to be the hard value here.
     * Block size cannot overcome 4MB limit (default). Can be configured as "io.compression.codec.4mc.blocksize"
     */
    protected int getCompressionBlockSize() {
        return FOURMC_MAX_BLOCK_SIZE;
        /*int bufferSize = getConf().getInt(FOURMZ_BLOCK_SIZE_KEY, FOURMZ_MAX_BLOCK_SIZE);
        if (bufferSize<1024*100 || bufferSize>FOURMZ_MAX_BLOCK_SIZE) {
            bufferSize = FOURMZ_MAX_BLOCK_SIZE;
        }
        return bufferSize;*/
    }
}
