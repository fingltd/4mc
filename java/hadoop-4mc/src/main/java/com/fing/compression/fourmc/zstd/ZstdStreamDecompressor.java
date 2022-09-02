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
package com.fing.compression.fourmc.zstd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Zstd Stream Decompressor.
 */
public class ZstdStreamDecompressor implements Decompressor {

    private static final Log LOG = LogFactory.getLog(ZstdStreamDecompressor.class.getName());

    private byte[] userBuf = null;
    private int userBufOff = 0;
    private int userBufLen = 0;
    private boolean finished;

    /* Opaque pointer to ZSTD_DStream context */
    private long dStream;

    /* Some constants */
    private final static int frameHeaderSizePrefix = 5; // Read size after initDStream.

    /* Input and output buffer size in decompressor.
     * It can be any sizes, here uses recommended sizes from ZSTD to reduce feeding and flush latency.
     */
    private final static int iBuffSize = (int) Zstd.dStreamInSize();
    private final static int oBuffSize = (int) Zstd.dStreamOutSize();

    private ByteBuffer iBuff = null;
    private int iBuffLen = 0;
    private long srcPos = 0;
    private ByteBuffer oBuff = null;
    private int oBuffLen = 0;
    private long dstPos = 0;
    private int toRead;

    private static boolean nativeLoaded = false;
    static {
        try {
            if (Zstd.isNativeLoaded()) {
                initIDs();
                nativeLoaded = true;
            }
        } catch (Throwable t) {
            nativeLoaded = false;
            LOG.error(t);
        }
    }
    /**
     * Check if native code is loaded..
     *
     * @return <code>true</code> if native lib is loaded & initialized,
     * else <code>false</code>
     */
    public static boolean isNativeLoaded() {
        return nativeLoaded;
    }

    /**
     * Creates a new ZstdStreamDecompressor.
     *
     */
    public ZstdStreamDecompressor() {
        dStream = createDStream();

        /*
         * Notice for developers:
         * Is a direct buffer pool needed here?
         * The iBuffSize and oBuffSize is about ~128K, which is way smaller than 4MB. Take the community Lz4Compressor
         * for reference, allocateDirect shall not trigger a java.lang.OutOfMemoryError: Direct Buffer Memory exception.
         *
         * We can use a direct buffer pool if needed.
         */
        iBuff = ByteBuffer.allocateDirect(iBuffSize);
        oBuff = ByteBuffer.allocateDirect(oBuffSize);
        reset();
    }

    public synchronized void setInput(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        this.userBuf = b;
        this.userBufOff = off;
        this.userBufLen = len;

        setInputFromSavedData();

        // Reinitialize Zstd output direct-buffer
        oBuff.limit(oBuffSize);
        oBuff.position(oBuffSize);
    }

    synchronized void setInputFromSavedData() {

        int len = Math.min(userBufLen, toRead - iBuff.position());

        ((ByteBuffer) iBuff).put(userBuf, userBufOff, len);

        userBufOff += len;
        userBufLen -= len;
        iBuffLen = iBuff.position();
    }

    public synchronized void setDictionary(byte[] b, int off, int len) {
        // nop
    }

    public synchronized boolean needsInput() {
        // Consume remaining compressed data?
        if (oBuff.remaining() > 0) {
            return false;
        }

        if (iBuffLen < toRead) {
            // Check if we have consumed all user-input
            if (userBufLen <= 0) {
                return true;
            } else {
                setInputFromSavedData();
            }
        }

        return false;
    }

    public synchronized boolean needsDictionary() {
        return false;
    }

    public synchronized boolean finished() {
        return (finished && oBuff.remaining() == 0);
    }

    public synchronized int decompress(byte[] b, int off, int len)
            throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        int numBytes = 0;
        numBytes = oBuff.remaining();
        if (numBytes > 0) {
            numBytes = Math.min(numBytes, len);
            ((ByteBuffer) oBuff).get(b, off, numBytes);
            return numBytes;
        }

        // Check if there is data to decompress. When an end of frame is reached, decompress shall not call
        // decompressStream without initStream.
        if (srcPos < iBuffLen || (iBuffLen == toRead && !finished)) {

            // Re-initialize the ZstdStream's output direct-buffer
            oBuff.rewind();
            oBuff.limit(oBuffSize);
            dstPos = 0;

            // Decompress data, all the input should be consumed
            toRead = decompressStream(dStream, oBuff, oBuffSize, iBuff, iBuffLen);
            if (Zstd.isError(toRead)) {
                throw new InternalError("ZSTD decompressStream failed, due to: " + Zstd.getErrorName(toRead));
            }

            // If toRead is 0, then we have finished decoding a frame. Finished should be set to true.
            finished = toRead == 0;

            // Check if all data in iBuff is consumed.
            if(srcPos >= iBuffLen) {
                srcPos = 0;
                iBuffLen = 0;
                iBuff.clear();
                // toRead being 1 is a special case, meaning:
                // 1. zstd really need another one byte.
                // 2. zstd don't flush all the data into oBuff when oBuff is small.
                // When all the input is consumed and dstPos > 0, then toRead = 1 only happens in case 2.
                // This exception will be eliminated in later versions of zstd(>1.0.0). The following line then can
                // be safely removed or kept untouched as it will not be triggered.
                toRead = (toRead == 1 && dstPos != 0) ? 0 : toRead;
            }
            // Read most iBuffSize, works even for skippable frame(toRead can be any sizes between 1 to 4GB-1
            // in a skippable frame)
            toRead = Math.min(toRead, iBuffSize);
            numBytes = oBuffLen;
            oBuff.limit(numBytes);
            // Return atmost 'len' bytes
            numBytes = Math.min(numBytes, len);
            ((ByteBuffer) oBuff).get(b, off, numBytes);
        }

        return numBytes;
    }

    public synchronized int getRemaining() {
        return userBufLen;
    }


    public synchronized void reset() {
        finished = false;
        iBuffLen = 0;
        oBuff.limit(oBuffSize);
        oBuff.position(oBuffSize);
        userBufOff = userBufLen = 0;

        toRead = initDStream(dStream);
        if (Zstd.isError(toRead)) {
           LOG.error("InitDStream failed! Error is: " + Zstd.getErrorName(toRead));
        }
        // In zstd v1.0.0, initDStream is 0, but it will be changed to frameHeaderSizePrefix(5) in the following
        // release. So we add the related code in advance.
        if (toRead == 0) {
            toRead = frameHeaderSizePrefix;
        }
    }

    public synchronized void end() {
        freeDStream(dStream);
    }


    /* JNI methods */
    private static native void initIDs();
    private static native long createDStream();

    /* Notice for developers:
     * We are mapping size_t(in the c version) to int here. Why is that valid?
     * For normal cases, return code is bounded by default buffers sizes(~128K), which is way less then 2G(Int).
     * For error codes, return code is negative, but converted to size_t in the jni calls. There is no loss of
     * information.
     *
     * Also remember: this code is intended to work on normal machines(Windows/Mac/Linux on commodity hardware)
     * Portability is not the major concern.
     */

    private static native int freeDStream(long stream);
    private static native int initDStream(long stream);
    private native int decompressStream(long stream, ByteBuffer dst, int dstSize, ByteBuffer src, int srcSize);

}

