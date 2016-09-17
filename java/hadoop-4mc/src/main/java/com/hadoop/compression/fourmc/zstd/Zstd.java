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

package com.hadoop.compression.fourmc.zstd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hadoop.compression.fourmc.FourMcNativeCodeLoader;

/**
 * @author Xianjin YE <yexianjin@baidu.com>
 * @since 7/27/16
 */

public class Zstd {
    private static final Log LOG = LogFactory.getLog(Zstd.class.getName());
    private static boolean nativeLoaded;

    static {
        if(FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            nativeLoaded = true;
        } else {
            LOG.error("Cannot load " + Zstd.class.getName() + " without libhadoop-4mc");
            nativeLoaded = false;
        }
    }

    public static boolean isNativeLoaded() {
        return nativeLoaded;
    }


    public static native boolean isError(long code);
    public static native String  getErrorName(long code);


    /*===== Stream Compression/Decompression part =====*/

    /**
     * @return recommended size for input buffer in stream compression
     */
    public static native long cStreamInSize();

    /**
     * @return recommended size for output buffer in stream compression
     */
    public static native long cStreamOutSize();

    /**
     * @return recommended size for input buffer in stream decompression
     */
    public static native long dStreamInSize();

    /**
     * @return recommended size for output buffer in stream decompression
     */
    public static native long dStreamOutSize();

}
