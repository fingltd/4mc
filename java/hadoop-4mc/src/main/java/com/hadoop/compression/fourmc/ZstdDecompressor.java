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

import com.hadoop.compression.fourmc.util.DirectBufferPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * ZSTD Decompressor, tuned for 4mc purposes.
 */
public class ZstdDecompressor implements Decompressor {

    private static final Log LOG = LogFactory.getLog(ZstdDecompressor.class.getName());

    private int directBufferSize;
    private Buffer compressedDirectBuf = null;
    private int compressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finished;

    private boolean isCurrentBlockUncompressed;

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
            LOG.error("Cannot load " + ZstdDecompressor.class.getName() +
                    " without native-hadoop library!");
            nativeLoaded = false;

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
     * Creates a new LZ4Decompressor.
     *
     * @param directBufferSize size of the direct-buffer
     */
    public ZstdDecompressor(int directBufferSize) {
        this.directBufferSize = directBufferSize;

        compressedDirectBuf = DirectBufferPool.getInstance().allocate(directBufferSize);
        uncompressedDirectBuf = DirectBufferPool.getInstance().allocate(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
    }

    /**
     * Creates a new LZ4Decompressor.
     */
    public ZstdDecompressor() {
        this(1024 * 1024 * 4);
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

        // Reinitialize ZSTD output direct-buffer
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
    }

    synchronized void setInputFromSavedData() {

        if (!isCurrentBlockUncompressed()) {
            compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

            if (compressedDirectBuf == null) {
                compressedDirectBuf = DirectBufferPool.getInstance().allocate(directBufferSize);
            }
            compressedDirectBuf.rewind();
            ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
                    compressedDirectBufLen);

            userBufOff += compressedDirectBufLen;
            userBufLen -= compressedDirectBufLen;
        }
    }

    public synchronized void setDictionary(byte[] b, int off, int len) {
        // nop
    }

    public synchronized boolean needsInput() {
        // Consume remaining compressed data?
        if (uncompressedDirectBuf.remaining() > 0) {
            return false;
        }

        if (compressedDirectBufLen <= 0) {
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
        return (finished && uncompressedDirectBuf.remaining() == 0);
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
        if (isCurrentBlockUncompressed()) {
            // The current block has been stored uncompressed, so just
            // copy directly from the input buffer.
            numBytes = Math.min(userBufLen, len);
            System.arraycopy(userBuf, userBufOff, b, off, numBytes);
            userBufOff += numBytes;
            userBufLen -= numBytes;
        } else {
            // Check if there is uncompressed data
            numBytes = uncompressedDirectBuf.remaining();
            if (numBytes > 0) {
                numBytes = Math.min(numBytes, len);
                ((ByteBuffer) uncompressedDirectBuf).get(b, off, numBytes);
                return numBytes;
            }

            // Check if there is data to decompress
            if (compressedDirectBufLen > 0) {
                // Re-initialize the LZ4's output direct-buffer
                uncompressedDirectBuf.rewind();
                uncompressedDirectBuf.limit(directBufferSize);

                // Decompress data
                numBytes = decompressBytesDirect();
                uncompressedDirectBuf.limit(numBytes);

                // Return atmost 'len' bytes
                numBytes = Math.min(numBytes, len);
                ((ByteBuffer) uncompressedDirectBuf).get(b, off, numBytes);
            }
        }

        // Set 'finished' if LZ4 has consumed all user-data
        if (userBufLen <= 0) {
            finished = true;
        }

        return numBytes;
    }

    public synchronized int getRemaining() {
        return userBufLen;
    }


    public synchronized void reset() {
        finished = false;
        compressedDirectBufLen = 0;
        if (uncompressedDirectBuf == null) {
            uncompressedDirectBuf = DirectBufferPool.getInstance().allocate(directBufferSize);
        }
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        userBufOff = userBufLen = 0;
    }

    public synchronized void end() {
        // nop
    }

    @Override
    protected void finalize() {
        try {
            super.finalize();
        } catch (Throwable ignored) {
        }
        end();
        releaseDirectBuffers();
    }

    // trying to get rid of java.lang.OufOfMemoryError: Direct Buffer Memory
    public void releaseDirectBuffers() {
        if (compressedDirectBuf != null) {
            DirectBufferPool.getInstance().release((ByteBuffer) compressedDirectBuf);
            compressedDirectBuf=null;
        }
        if (uncompressedDirectBuf != null) {
            DirectBufferPool.getInstance().release((ByteBuffer) uncompressedDirectBuf);
            uncompressedDirectBuf=null;
        }
    }

    /**
     * Note whether the current block being decompressed is actually
     * stored as uncompressed data.  If it is, there is no need to
     * use the LZ4 decompressor, and no need to update compressed
     * checksums.
     *
     * @param uncompressed Whether the current block of data is uncompressed already.
     */
    public synchronized void setCurrentBlockUncompressed(boolean uncompressed) {
        isCurrentBlockUncompressed = uncompressed;
    }

    /**
     * Query the compression status of the current block as it exists
     * in the file.
     *
     * @return true if the current block of data was stored as uncompressed.
     */
    protected synchronized boolean isCurrentBlockUncompressed() {
        return isCurrentBlockUncompressed;
    }

    private native static void initIDs();
    private native int decompressBytesDirect();

    public native static int xxhash32(byte[] input, int offset, int len, int seed);
}
