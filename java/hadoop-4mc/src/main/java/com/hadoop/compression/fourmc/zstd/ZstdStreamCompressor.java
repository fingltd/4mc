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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ZSTD Streaming Compressor.
 */
public class ZstdStreamCompressor implements Compressor {
    private static final Log LOG = LogFactory.getLog(ZstdStreamCompressor.class.getName());

    private byte[] userBuf = null;
    private int userBufOff = 0;
    private int userBufLen = 0;
    private boolean finish;
    private boolean finished;
    private int compressionLevel;

    private long bytesRead = 0L;
    private long bytesWritten = 0L;

    /* Opaque pointer to ZSTD_CStream context */
    private long cStream;

    /* Bytes remaining to flush after an endFrame call.
     * Some bytes might be left within internal buffer if oBuffSize is too small.
     * In which case, multiple endFrame calls will be made.
     */
    private int remainingToFlush = 0;

    /* Input and output buffer size in compressor.
     * It can be any sizes, here uses recommended sizes from ZSTD to reduce feeding and flush latency.
     */
    private final static int iBuffSize = (int) Zstd.cStreamInSize();
    private final static int oBuffSize = (int) Zstd.cStreamOutSize();

    private ByteBuffer iBuff = null;
    private int iBuffLen = 0;
    private long srcPos = 0;
    private ByteBuffer oBuff = null;
    private int oBuffLen = 0;
    private long dstPos = 0;

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

    public static boolean isNativeLoaded() {
        return nativeLoaded;
    }


    public ZstdStreamCompressor() {
        this(1);
    }

    public ZstdStreamCompressor(int compressionLevel) {
        this.compressionLevel = compressionLevel;
        init();
    }


    private void init() {
        cStream = createCStream();

        /*
         * Notice for developers:
         * Is a direct buffer pool needed here?
         * The iBuffSize and oBuffSize is about ~128K, which is way smaller than 4MB. Take the community Lz4Compressor
         * for reference, allocateDirect shall not trigger a java.lang.OutOfMemoryError: Direct Buffer Memory exception.
         *
         * We can always use a direct buffer pool if needed.
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
        finished = false;

        this.userBuf = b;
        this.userBufOff = off;
        this.userBufLen = len;
        setInputFromSavedData();
        oBuff.limit(oBuffSize);
        oBuff.position(oBuffSize);
        bytesRead += len;
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    synchronized void setInputFromSavedData() {
        int len = Math.min(userBufLen, iBuff.remaining());

        ((ByteBuffer) iBuff).put(userBuf, userBufOff, len);

        // Note how much data is being fed to zstd
        userBufOff += len;
        userBufLen -= len;
        iBuffLen = iBuff.position();
    }

    public synchronized void setDictionary(byte[] b, int off, int len) {
        // nop
    }

    /**
     * {@inheritDoc}
     */
    public boolean needsInput() {
        // There is unconsumed compressed data.
        if (oBuff.remaining() > 0) {
            return false;
        }

        if (iBuff.remaining() > 0) {
            // Is all user input consumed?
            if (userBufLen <= 0) {
                return true;
            } else {
                // Fill inputBuffer
                setInputFromSavedData();
                if (iBuff.remaining() > 0) // iBuff is not full, more data is needed.
                    return true;
                else
                    return false;
            }
        }
        return false;

    }

    public synchronized void finish() {
        finish = true;
    }

    public synchronized boolean finished() {
        return (finish && finished && oBuff.remaining() == 0);
    }

    public synchronized int compress(byte[] b, int off, int len)
            throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        int n = oBuff.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) oBuff).get(b, off, n);
            bytesWritten += n;
            return n;
        }
        // Happens when oBuffSize is small, zstd cannot flush content in the internal buffer just once.
        // The code will not be triggered if we use Zstd.cStreamInSize/Zstd.cStreamOutSize as input/output buffer size
        if (remainingToFlush > 0) {
            oBuff.rewind();
            remainingToFlush = endStream(cStream, oBuff, 0, oBuff.capacity());
            if (Zstd.isError(remainingToFlush)) {
                throw new InternalError("Zstd endStream failed, due to: " + Zstd.getErrorName(remainingToFlush));
            }
            finished = remainingToFlush == 0;
            oBuff.limit(oBuffLen);
            n = Math.min(oBuffLen, len);
            bytesWritten += n;
            ((ByteBuffer) oBuff).get(b, off, n);

            return n;
        }
        if (0 == iBuff.position()) {
            setInputFromSavedData();
            if (0 == iBuff.position()) {
                finished = true;
                return 0;
            }
        }

        oBuff.rewind();
        oBuff.limit(oBuffSize);

        // iBuffLen = iBuffSize in most times. iBuffLen can be < iBuffSize if compress() is called after finish();
        // oBuff is cleared before this call.
        int toRead = compressStream(cStream, oBuff, oBuffSize, iBuff, iBuffLen);

        if (Zstd.isError(toRead)) {
            throw new InternalError("ZSTD compressStream failed, due to: " + Zstd.getErrorName(toRead));
        }
        boolean inputConsumedAll = srcPos >= iBuffLen;
        // If all the data in iBuff is consumed, then iBuff should be reset.
        // Otherwise, data in iBuff remains intact and will be consumed by compressStream in the next compress() call
        if (inputConsumedAll) {
            iBuff.clear();
            srcPos = 0;
            iBuffLen = 0;
        }

        // finish() is called, all the data in iBuffLen is consumed, then a endFrame epilogue should be wrote.
        if (finish && userBufLen <= 0 && inputConsumedAll) {
            int oBuffOffset = oBuffLen;
            remainingToFlush = endStream(cStream, oBuff, oBuffOffset, oBuff.capacity() - oBuffOffset);
            if (Zstd.isError(remainingToFlush)) {
                throw new InternalError("Zstd endStream failed, due to: " + Zstd.getErrorName(remainingToFlush));
            }
            finished = remainingToFlush == 0;
        }

        oBuff.limit(oBuffLen);
        n = Math.min(oBuffLen, len);
        bytesWritten += n;
        ((ByteBuffer) oBuff).get(b, off, n);

        return n;
    }

    public synchronized void reset() {
        finish = false;
        finished = false;
        userBufOff = userBufLen = 0;
        srcPos = dstPos = 0;

        int r = initCStream(cStream, compressionLevel);
        if (Zstd.isError(r)) {
            LOG.error("CompressInit failed! Error is:" +  Zstd.getErrorName(r));
        }
    }


    /**
     * Return number of bytes given to this compressor since last reset.
     */
    public synchronized long getBytesRead() {
        return bytesRead;
    }

    /**
     * Return number of bytes consumed by callers of compress since last reset.
     */
    public synchronized long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * freeCStream
     */
    public synchronized void end() {
        freeCStream(cStream);
    }

    @Override
    public void reinit(Configuration configuration) {
        reset();
    }

    /* JNI methods */
    private static native void initIDs();
    private static native long createCStream();

    /* Notice for developers:
     * We are mapping size_t(in the c version) to int here. Why is that valid?
     * For normal cases, return code is bounded by default buffers sizes(~128K), which is way less than 2G(Int).
     * For error codes, return code is negative, but converted to size_t in the jni calls. There is no loss of
     * information.
     *
     * Also remember: this code is intended to work on normal machines(Windows/Mac/Linux on commodity hardware)
     * Portability is not the major concern.
     */

    private static native int freeCStream(long stream);
    private static native int initCStream(long stream, int level);
    private native int compressStream(long stream, ByteBuffer dst, int dstSize,
                                      ByteBuffer src, int srcSize);
    // private native int flushStream(long ctx, ByteBuffer dst, int dstSize);
    private native int endStream(long stream, ByteBuffer dst, int dstOffset, int dstSize);
}

