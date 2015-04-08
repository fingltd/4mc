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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;


/**
 * Reads 4mc files, compressed block format leveraging LZ4 compression power.
 */
public class FourMcInputStream extends BlockDecompressorStream {

    private static final Log LOG = LogFactory.getLog(FourMcInputStream.class);

    static {
        if (FourMcNativeCodeLoader.isNativeCodeLoaded()) {
            boolean nativeLoaded = Lz4Decompressor.isNativeLoaded();

            if (!nativeLoaded) {
                LOG.error("Failed to load/initialize native-4mc library");
            }
        } else {
            LOG.error("Cannot load native-4mc without native-hadoop");
        }
    }

    private final byte[] buf = new byte[12];

    private int noUncompressedBytes = 0;
    private int noCompressedBytes = 0;
    private int uncompressedBlockSize = 0;

    public FourMcInputStream(InputStream in, Decompressor decompressor,
                             int bufferSize) throws IOException {
        super(in, decompressor, bufferSize);
        readHeader(in);
    }


    /**
     * Reads len bytes in a loop.
     * <p/>
     * This is copied from IOUtils.readFully except that it throws an EOFException
     * instead of generic IOException on EOF.
     *
     * @param in  The InputStream to read from
     * @param buf The buffer to fill
     * @param off offset from the buffer
     * @param len the length of bytes to read
     */
    private static void readFully(InputStream in, byte buf[],
                                  int off, int len) throws IOException, EOFException {
        int toRead = len;
        while (toRead > 0) {
            int ret = in.read(buf, off, toRead);
            if (ret < 0) {
                throw new EOFException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    /**
     * Read len bytes into buf, st LSB of int returned is the last byte of the
     * first word read.
     */
    private static int readInt(InputStream in, byte[] buf, int len)
            throws IOException {
        readFully(in, buf, 0, len);
        int ret = (0xFF & buf[0]) << 24;
        ret |= (0xFF & buf[1]) << 16;
        ret |= (0xFF & buf[2]) << 8;
        ret |= (0xFF & buf[3]);
        return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }

    private static int getInt(byte[] buf, int offset)
            throws IOException {
        int ret = (0xFF & buf[offset]) << 24;
        ret |= (0xFF & buf[offset + 1]) << 16;
        ret |= (0xFF & buf[offset + 2]) << 8;
        ret |= (0xFF & buf[offset + 3]);
        return ret;
    }

    /**
     * Read and verify 4mc header.
     */
    protected void readHeader(InputStream in) throws IOException {

        readFully(in, buf, 0, 12);
        int magic = getInt(buf, 0);
        if (magic != FourMcCodec.FOURMC_MAGIC) {
            throw new IOException("Invalid 4mc header (wrong magic)");
        }
        int version = getInt(buf, 4);
        if (version != FourMcCodec.FOURMC_VERSION) {
            throw new IOException("Invalid 4mc header (wrong version)");
        }
        int hdrChecksum = getInt(buf, 8);
        if (hdrChecksum != Lz4Decompressor.xxhash32(buf, 0, 8, 0)) {
            throw new IOException("Invalid 4mc header (invalid checksum)");
        }
    }

    /**
     * Reads blocks index at tail of file.
     *
     * @param fs   filesystem
     * @param file path to 4mc file
     * @return block index
     * @throws IOException
     */
    public static FourMcBlockIndex readIndex(FileSystem fs, Path file) throws IOException {

        long fileSize = fs.getFileStatus(file).getLen();
        if (fileSize < (12 + 20)) { // file too small
            return new FourMcBlockIndex();
        }

        FSDataInputStream indexIn = fs.open(file);

        /*
            4mc Footer:
             Footer size:        4 bytes
             Footer version:     4 byte (1)
             Block index offset: 4 bytes delta offset for each stored block, the delta between offset between previous file position and next block
             Footer size:        4 bytes (repeated to be able to read from end of file)
             MAGIC SIGNATURE:    4 bytes: "4MC\0"
             Footer checksum:    4 bytes (always in XXHASH32)

        */

        /**
         * jump to file tail and read-ahead last 4KB of file which should be enough in most cases
         * Improvement: we could estimate a best case compression factor of 10% and calc forecast
         *              based on filesize and blocksize, to see if better to read-head more.
         */

        int readTailSize = 4 * 1024;
        if (readTailSize>(fileSize-12))
            readTailSize = (int)(fileSize-12);

        indexIn.seek(fileSize - readTailSize);
        byte[] buf = new byte[readTailSize];
        readFully(indexIn, buf, 0, buf.length);
        int footerSize =  getInt(buf, buf.length-12);
        int magic = getInt(buf, buf.length-8);
        int checksum = getInt(buf, buf.length - 4);

        if (magic != FourMcCodec.FOURMC_MAGIC) {
            throw new IOException("Invalid 4mc footer magic");
        }
        if (footerSize >= (fileSize - 12)) {
            throw new IOException("Invalid 4mc footer checksum");
        }

        // very rare case: read head was not enough! seek back and read it all
        if (footerSize>readTailSize) {
            readTailSize = footerSize;
            indexIn.seek(fileSize - readTailSize);
            buf = new byte[readTailSize];
            readFully(indexIn, buf, 0, buf.length);
        }
        indexIn.close();

        int startFooterOffset = readTailSize - footerSize;

        if (getInt(buf, startFooterOffset) != footerSize) { // size again
            throw new IOException("Invalid 4mc footer size");
        }

        if (getInt(buf, startFooterOffset+4) != FourMcCodec.FOURMC_VERSION) { // version
            throw new IOException("Invalid 4mc footer version (" + getInt(buf, startFooterOffset+4) + ")");
        }

        if (checksum != Lz4Decompressor.xxhash32(buf, startFooterOffset, footerSize - 4, 0)) {
            throw new IOException("Invalid 4mc footer checksum");
        }

        int totalBlocks = (footerSize - 20) / 4;
        FourMcBlockIndex index = new FourMcBlockIndex(totalBlocks);
        long curOffset = 0;
        for (int i = 0; i < totalBlocks; ++i) {
            curOffset += getInt(buf, startFooterOffset + 8 + (i * 4));
            index.set(i, curOffset);
        }

        return index;
    }


    @Override
    protected int decompress(byte[] b, int off, int len) throws IOException {
        // Check if we are the beginning of a block
        if (noUncompressedBytes == uncompressedBlockSize) {

            // Get original data size
            try {
                byte[] tempBuf = new byte[4];
                uncompressedBlockSize = readInt(in, tempBuf, 4);
                noCompressedBytes += 4;
            } catch (EOFException e) {
                return -1;
            }
            noUncompressedBytes = 0;
        }

        int n = 0;
        while ((n = decompressor.decompress(b, off, len)) == 0) {

            if (decompressor.finished() || decompressor.needsDictionary()) {
                if (noUncompressedBytes >= uncompressedBlockSize) {
                    eof = true;
                    return -1;
                }
            }

            if (decompressor.needsInput()) {
                try {
                    getCompressedData();
                } catch (EOFException e) {
                    eof = true;
                    return -1;
                } catch (IOException e) {
                    LOG.warn("IOException in getCompressedData; likely 4mc corruption.", e);
                    throw e;
                }
            }
        }

        // Note the no. of decompressed bytes read from 'current' block
        noUncompressedBytes += n;

        return n;
    }

    /**
     * Read checksums and feed compressed block data into decompressor.
     */
    @Override
    protected int getCompressedData() throws IOException {
        checkStream();

        // Get the size of the compressed chunk
        int compressedLen = readInt(in, buf, 4);
        noCompressedBytes += 4;

        // Get the checksum of the compressed chunk
        int checksum = readInt(in, buf, 4);
        noCompressedBytes += 4;

        if (compressedLen > FourMcCodec.FOURMC_MAX_BLOCK_SIZE) {
            throw new IOException("Compressed length " + compressedLen +
                    " exceeds max block size " + FourMcCodec.FOURMC_MAX_BLOCK_SIZE);
        }

        Lz4Decompressor lz4dec = (Lz4Decompressor) decompressor;

        // if compressed len == uncompressedBlockSize, 4mc wrote data w/o compression
        lz4dec.setCurrentBlockUncompressed(compressedLen >= uncompressedBlockSize);

        // Read len bytes from underlying stream
        if (compressedLen > buffer.length) {
            buffer = new byte[compressedLen];
        }
        readFully(in, buffer, 0, compressedLen);
        noCompressedBytes += compressedLen;

        // checksum check
        if (checksum != Lz4Decompressor.xxhash32(buffer, 0, compressedLen, 0)) {
            if (lz4dec.isCurrentBlockUncompressed()) {
                throw new IOException("Corrupted uncompressed block (invalid checksum)");
            } else {
                throw new IOException("Corrupted compressed block (invalid checksum)");
            }
        }

        // Send the read data to the decompressor.
        lz4dec.setInput(buffer, 0, compressedLen);

        return compressedLen;
    }

    public long getCompressedBytesRead() {
        return noCompressedBytes;
    }

    @Override
    public void close() throws IOException {
        byte[] b = new byte[4096];
        while (!decompressor.finished()) {
          decompressor.decompress(b, 0, b.length);
        }
        super.close();
    }
}

