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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * LZ4 Compressor (FAST).
 */
class Lz4Compressor implements Compressor {
    private static final Log LOG = LogFactory.getLog(Lz4Compressor.class.getName());

    private int directBufferSize;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private ByteBuffer uncompressedDirectBuf = null;
    private int uncompressedDirectBufLen = 0;
    private ByteBuffer compressedDirectBuf = null;
    private boolean finish, finished;

    private long bytesRead = 0L;
    private long bytesWritten = 0L;

    private static boolean nativeLoaded;

    static {
        if (FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            // Initialize the native library
            try {
                initIDs();
                nativeLoaded = true;
            } catch (Throwable t) {
                LOG.warn(t.toString());
                nativeLoaded = false;
            }
        } else {
            LOG.error("Cannot load " + Lz4Compressor.class.getName() +
                    " without native-hadoop library!");
            nativeLoaded = false;
        }
    }

    public static boolean isNativeLoaded() {
        return nativeLoaded;
    }

    public Lz4Compressor() {
        init(4 * 1024 * 1024);
    }

    /**
     * Reinitialize from a configuration, possibly changing compression codec
     */
    //@Override (this method isn't in vanilla 0.20.2, but is in CDH3b3 and YDH)
    public void reinit(Configuration conf) {
        init(this.directBufferSize);
    }


    public Lz4Compressor(int directBufferSize) {
        init(directBufferSize);
    }

    /**
     * Reallocates a direct byte buffer by freeing the old one and allocating
     * a new one, unless the size is the same, in which case it is simply
     * cleared and returned.
     * <p/>
     * NOTE: this uses unsafe APIs to manually free memory - if anyone else
     * has a reference to the 'buf' parameter they will likely read random
     * data or cause a segfault by accessing it.
     */
    private ByteBuffer realloc(ByteBuffer buf, int newSize) {
        if (buf != null) {
            if (buf.capacity() == newSize) {
                // Can use existing buffer
                buf.clear();
                return buf;
            }
            try {
                // Manually free the old buffer using undocumented unsafe APIs.
                // If this fails, we'll drop the reference and hope GC finds it
                // eventually.
                Object cleaner = buf.getClass().getMethod("cleaner").invoke(buf);
                cleaner.getClass().getMethod("clean").invoke(cleaner);
            } catch (Exception e) {
                // Perhaps a non-sun-derived JVM - contributions welcome
                LOG.warn("Couldn't realloc bytebuffer", e);
            }
        }
        return ByteBuffer.allocateDirect(newSize);
    }

    private void init(int directBufferSize) {
        this.directBufferSize = directBufferSize;
        int buffPlusOverhead = compressBound(directBufferSize);

        uncompressedDirectBuf = realloc(uncompressedDirectBuf, directBufferSize);
        compressedDirectBuf = realloc(compressedDirectBuf, buffPlusOverhead);
        compressedDirectBuf.position(buffPlusOverhead);
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

        if (len > uncompressedDirectBuf.remaining()) {
            // save data; now !needsInput
            this.userBuf = b;
            this.userBufOff = off;
            this.userBufLen = len;
        } else {
            ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
            uncompressedDirectBufLen = uncompressedDirectBuf.position();
        }
        bytesRead += len;
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    synchronized void setInputFromSavedData() {
        if (0 >= userBufLen) {
            return;
        }
        finished = false;

        uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
        ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
                uncompressedDirectBufLen);

        // Note how much data is being fed to lz4
        userBufOff += uncompressedDirectBufLen;
        userBufLen -= uncompressedDirectBufLen;
    }

    public synchronized void setDictionary(byte[] b, int off, int len) {
        // nop
    }

    /**
     * {@inheritDoc}
     */
    public boolean needsInput() {
        return !(compressedDirectBuf.remaining() > 0
                || uncompressedDirectBuf.remaining() == 0
                || userBufLen > 0);
    }

    public synchronized void finish() {
        finish = true;
    }

    public synchronized boolean finished() {
        return (finish && finished && compressedDirectBuf.remaining() == 0);
    }

    public int compressBytesDirectSpecific() {
        return compressBytesDirect();
    }

    public synchronized int compress(byte[] b, int off, int len)
            throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        int n = compressedDirectBuf.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) compressedDirectBuf).get(b, off, n);
            bytesWritten += n;
            return n;
        }

        compressedDirectBuf.clear();
        compressedDirectBuf.limit(0);
        if (0 == uncompressedDirectBuf.position()) {
            setInputFromSavedData();
            if (0 == uncompressedDirectBuf.position()) {
                finished = true;
                return 0;
            }
        }

        n = compressBytesDirectSpecific();
        compressedDirectBuf.limit(n);
        uncompressedDirectBuf.clear();

        if (0 == userBufLen) {
            finished = true;
        }

        n = Math.min(n, len);
        bytesWritten += n;
        ((ByteBuffer) compressedDirectBuf).get(b, off, n);

        return n;
    }

    public synchronized void reset() {
        finish = false;
        finished = false;
        uncompressedDirectBuf.clear();
        uncompressedDirectBufLen = 0;
        compressedDirectBuf.clear();
        compressedDirectBuf.limit(0);
        userBufOff = userBufLen = 0;
        bytesRead = bytesWritten = 0L;
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
     * Exactly like LZO, 4mc format wants uncompressed block if compression cannot reduce its size.
     * So this returns the uncompressed original data, in case it's needed.
     */
    public byte[] uncompressedBytes() {
        byte[] b = new byte[(int) bytesRead];
        ((ByteBuffer) uncompressedDirectBuf).get(b);
        return b;
    }

    /**
     * Used only by tests.
     */
    long getDirectBufferSize() {
        return directBufferSize;
    }

    /**
     * Noop.
     */
    public synchronized void end() {
        // nop
    }

    private native static void initIDs();

    public native static int xxhash32(byte[] input, int offset, int len, int seed);
    public native static int compressBound(int origSize);

    protected native int compressBytesDirect();
    protected native int compressBytesDirectMC();
    protected native int compressBytesDirectHC(int level);

}

