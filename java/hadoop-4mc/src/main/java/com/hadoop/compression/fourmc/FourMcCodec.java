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

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} leveraging 4mc format.
 * 4MC (Four More Compression) - LZ4 power unleashed.<br>
 * <b>Fast Compression:</b> default one reaching up to 500 MB/s (LZ4 fast)<br>
 * <b>Medium Compression:</b> half speed of fast mode, +13% ratio (LZ4 MC)<br>
 * <b>High Compression:</b> 5x slower than fast, +25% ratio (LZ4 HC lvl 4)<br>
 * <b>Ultra Compression:</b> 13x slower than fast, +30% ratio (LZ4 HC lvl 8)<br>
 *
 * <br><br>This class provides default <b>Fast</b> implementation.<br><br>
 *
 * Bechmark with silesia on Linux CentOS 6.4 64bit - HP DL 380P Intel(R) Xeon(R) CPU E5-2697 v2 @ 2.70GHz<br>
 * Algorithm     Compression Speed     Decompression Speed      Ratio <br>
 * Fast                   390 MB/s               2200 MB/s      2.084 <br>
 * Medium                 170 MB/s               2206 MB/s      2.340 <br>
 * High                    69 MB/s               2436 MB/s      2.630 <br>
 * Ultra                   30 MB/s               2515 MB/s      2.716 <br>
 *<br><br>
 * 4mc file format for reference:
 * Header:
 * <p/>
 * MAGIC SIGNATURE:  4 bytes: "4MC\0"
 * Version:          4 byte (1)
 * Header checksum:  4 bytes
 * <p/>
 * Blocks:
 * Uncompressed size:  4 bytes
 * Compressed size:    4 bytes, if compressed size==uncompressed size, then the data is stored as plain
 * Checksum:           4 bytes, calculated on the compressed data
 * <p/>
 * Footer:
 * Footer size:        4 bytes
 * Footer version:     4 byte (1)
 * Block index offset: 4 bytes delta offset for each stored block, the delta between offset between previous file position and next block
 * Footer size:        4 bytes (repeated to be able to read from end of file)
 * MAGIC SIGNATURE:    4 bytes: "4MC\0"
 * Footer checksum:    4 bytes (always in XXHASH32)
 */
public class FourMcCodec extends Lz4Codec {

    public static final int FOURMC_MAGIC = 0x344D4300;
    public static final int FOURMC_VERSION = 1;
    public static final int FOURMC_MAX_BLOCK_SIZE = 4 * 1024 * 1024;
    public static final String FOURMC_DEFAULT_EXTENSION = ".4mc";
    public static final String FOURMC_BLOCK_SIZE_KEY = "io.compression.codec.4mc.blocksize";

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return createOutputStream(out, createCompressor());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new FourMcOutputStream(out, compressor, FOURMC_MAX_BLOCK_SIZE);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new FourMcInputStream(in, decompressor, FOURMC_MAX_BLOCK_SIZE);
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
        return Lz4Decompressor.class;
    }

    @Override
    public Decompressor createDecompressor() {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new Lz4Decompressor(FOURMC_MAX_BLOCK_SIZE);
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return Lz4Compressor.class;
    }

    @Override
    public Compressor createCompressor() {
        assert getConf() != null : "Configuration cannot be null! You must call setConf() before creating a compressor.";
        if (!isNativeLoaded(getConf())) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new Lz4Compressor(getCompressionBlockSize());
    }


    @Override
    public String getDefaultExtension() {
        return FOURMC_DEFAULT_EXTENSION;
    }

    /**
     * 4MB has been decided to be the hard value here.
     * Block size cannot overcome 4MB limit (default). Can be configured as "io.compression.codec.4mc.blocksize"
     */
    protected int getCompressionBlockSize() {
        return FOURMC_MAX_BLOCK_SIZE;
        /*int bufferSize = getConf().getInt(FOURMC_BLOCK_SIZE_KEY, FOURMC_MAX_BLOCK_SIZE);
        if (bufferSize<1024*100 || bufferSize>FOURMC_MAX_BLOCK_SIZE) {
            bufferSize = FOURMC_MAX_BLOCK_SIZE;
        }
        return bufferSize;*/
    }
}
