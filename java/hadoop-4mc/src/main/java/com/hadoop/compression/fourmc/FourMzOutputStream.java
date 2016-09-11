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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes with 4mz files, compressed block format leveraging ZSTD compression power.
 */
public class FourMzOutputStream extends CompressorStream {

    private static final Log LOG = LogFactory.getLog(FourMzOutputStream.class);

    private List<Long> blockOffsets;
    private CountingOutputStream cout;

    static {
        if (FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            boolean nativeLoaded = ZstdCompressor.isNativeLoaded();
            if (!nativeLoaded) {
                LOG.error("Failed to load/initialize native-4mc library");
            }
        } else {
            LOG.error("Cannot load native-4mc without native-hadoop");
        }
    }

    protected static void write4mzHeader(OutputStream out) throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();
        try {
            dob.writeInt(FourMzCodec.FOURMZ_MAGIC);
            dob.writeInt(FourMzCodec.FOURMZ_VERSION);
            int checksum = ZstdCompressor.xxhash32(dob.getData(),0,8,0);
            dob.writeInt(checksum);
            out.write(dob.getData(), 0, dob.getLength());
        } finally {
            dob.close();
        }
    }

    public FourMzOutputStream(OutputStream out, Compressor compressor, int bufferSize)  throws IOException {
        super(new CountingOutputStream(out), compressor, bufferSize);

        this.cout = (CountingOutputStream) this.out;
        this.blockOffsets = new ArrayList<Long>(32);

        try {
            write4mzHeader(this.out);
        } catch (IOException e) {
            // force release compressor and related direct buffers
            ((ZstdCompressor)this.compressor).releaseDirectBuffers();
            this.compressor=null;
            throw e;
        }
    }

    /**
     * Before closing the stream, 4mc footer must be written.
     */
    @Override
    public void close() throws IOException {
        if (closed) return;

        finish();

        // write last block marker
        rawWriteInt(0);
        rawWriteInt(0);
        rawWriteInt(0);

        // time to write footer with block index
        int footerSize = 20 + blockOffsets.size()*4;
        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeInt(footerSize);
        dob.writeInt(FourMzCodec.FOURMZ_VERSION);

        // write block deltas
        for (int i=0; i< blockOffsets.size(); ++i) {
            long blockDelta = i==0 ? ( blockOffsets.get(i) ) : (blockOffsets.get(i) - blockOffsets.get(i-1));
            dob.writeInt((int)blockDelta);
        }

        // tail of footer and checksum
        dob.writeInt(footerSize);
        dob.writeInt(FourMzCodec.FOURMZ_MAGIC);
        int checksum = ZstdCompressor.xxhash32(dob.getData(),0,dob.getLength(),0);
        dob.writeInt(checksum);
        out.write(dob.getData(), 0, dob.getLength());

        out.close();
        closed = true;

        // force release compressor and related direct buffers
        ((ZstdCompressor)compressor).releaseDirectBuffers();
        compressor=null;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // exactly like the  case of LzopOutputStream this is a bit complex
        // to be able to handle custom needs of block compression and related block indexes

        // Sanity checks
        if (compressor.finished()) {
            throw new IOException("write beyond end of stream");
        }
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        long limlen = compressor.getBytesRead();
        if (len + limlen > FourMzCodec.FOURMC_MAX_BLOCK_SIZE && limlen > 0) {
            finish();
            compressor.reset();
        }

        if (len > FourMzCodec.FOURMC_MAX_BLOCK_SIZE) {
            do {
                int bufLen = Math.min(len, FourMzCodec.FOURMC_MAX_BLOCK_SIZE);
                compressor.setInput(b, off, bufLen);
                finish();
                compressor.reset();
                off += bufLen;
                len -= bufLen;
            } while (len > 0);
            return;
        }

        // Give data to the compressor
        compressor.setInput(b, off, len);
        if (!compressor.needsInput()) {
            do {
                compress();
            } while (!compressor.needsInput());
        }
    }

    @Override
    public void finish() throws IOException {
        if (!compressor.finished()) {
            compressor.finish();
            while (!compressor.finished()) {
                compress();
            }
        }
    }

    @Override
    protected void compress() throws IOException {
        int len = compressor.compress(buffer, 0, buffer.length);

        if (len > 0) {
            // new block. take current position to for block index
            blockOffsets.add(cout.bytesWritten);

            rawWriteInt((int) compressor.getBytesRead());

            if (compressor.getBytesRead() <= compressor.getBytesWritten()) {
                // write uncompressed data block
                byte[] uncompressed = ((ZstdCompressor) compressor).uncompressedBytes();
                rawWriteInt(uncompressed.length);
                int checksum = ZstdCompressor.xxhash32(uncompressed, 0, uncompressed.length, 0);
                rawWriteInt(checksum);
                out.write(uncompressed, 0, uncompressed.length);

                // fix by Xianjin YE (advancedxy) to https://github.com/carlomedas/4mc/issues/12
                compressor.reset(); // reset compressor buffers
                compressor.finish(); // set compressor to be finished.

            } else {     // write compressed data block
                rawWriteInt(len);
                int checksum = ZstdCompressor.xxhash32(buffer, 0, len, 0);
                rawWriteInt(checksum);
                out.write(buffer, 0, len);
            }
        }
    }

    private void rawWriteInt(int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v ) & 0xFF);
    }

    /* keeps count of number of bytes written. */
    private static class CountingOutputStream extends FilterOutputStream {
        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        long bytesWritten = 0;

        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            bytesWritten += len;
        }

        public void write(int b) throws IOException {
            out.write(b);
            bytesWritten++;
        }
    }
}

