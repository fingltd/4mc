/*
 * Copyright (c) 2016-2016, Xianjin YE(advancedxy@gmail.com)
 * BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *   list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package com.hadoop.compression.fourmc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.hadoop.compression.fourmc.zstd.ZstdStreamCompressor;
import com.hadoop.compression.fourmc.zstd.ZstdStreamDecompressor;

/**
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} for a streaming
 * <b>Zstd</b> compression/decompression pair.
 * see https://github.com/Cyan4973/zstd for more details about the compression algorithm.
 */
public class ZstCodec implements Configurable, CompressionCodec {
    private static final Log LOG = LogFactory.getLog(ZstCodec.class.getName());

    // buffer size can be any size, let's make it reasonable and set it to 256k.
    public static final int ZST_BUFFER_SIZE = 256 * 1024;

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private static boolean nativeLoaded = false;

    static {
        if (FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            nativeLoaded = ZstdStreamCompressor.isNativeLoaded() && ZstdStreamDecompressor.isNativeLoaded();

            if (nativeLoaded) {
                LOG.info("Successfully loaded & initialized hadoop-4mc library");
            } else {
                LOG.error("Failed to load/initialize hadoop-4mc library");
            }
        } else {
            LOG.error("Cannot load hadoop-4mc native library");
        }
    }

    public static boolean isNativeLoaded(Configuration conf) {
        assert conf != null : "Configuration cannot be null!";
        return nativeLoaded && conf.getBoolean("hadoop.native.lib", true);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return createOutputStream(out, createCompressor());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor) throws IOException {

        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new CompressorStream(out, compressor, ZST_BUFFER_SIZE);
    }


    @Override
    public Class<? extends Compressor> getCompressorType() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdStreamCompressor.class;
    }

    @Override
    public Compressor createCompressor() {
        assert conf != null : "Configuration cannot be null! You must call setConf() before creating a compressor.";
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        int compressionLevel = conf.getInt("io.compress.zst.compression.level", 1);
        if (compressionLevel <= 0 || compressionLevel >= 23) {
            compressionLevel = 3;
        }
        return new ZstdStreamCompressor(compressionLevel);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException {
        return createInputStream(in, createDecompressor());
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in,
                                                    Decompressor decompressor)
            throws IOException {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return new DecompressorStream(in, decompressor, ZST_BUFFER_SIZE);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }
        return ZstdStreamDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor() {
        if (!isNativeLoaded(conf)) {
            throw new RuntimeException("native hadoop-4mc library not available");
        }

        return new ZstdStreamDecompressor();
    }

    /**
     * Get the default filename extension for this kind of compression.
     *
     * @return the extension including the '.'
     */
    @Override
    public String getDefaultExtension() {
        return ".zst";
    }

}

