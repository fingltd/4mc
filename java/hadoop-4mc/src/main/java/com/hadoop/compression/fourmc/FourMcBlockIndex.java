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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;


/**
 * 4mc file contains in its footer the blocks index.
 * For each compressed data chunk, the relative offset is stored as dword.
 * This is helper class used to work with block index, actually read by FourMcInputStream.
 */
public class FourMcBlockIndex {
    public static final long NOT_FOUND = -1;

    private long[] blockOffsets;

    /**
     * Create an empty index, typically indicating no index file exists.
     */
    public FourMcBlockIndex() {
    }

    /**
     * Create an index specifying the number of blocks.
     */
    public FourMcBlockIndex(int blocks) {
        blockOffsets = new long[blocks];
    }

    /**
     * Set the absolute offset for the provided block.
     */
    public void set(int blockNo, long offset) {
        blockOffsets[blockNo] = offset;
    }

    /**
     * Get the total number of blocks in the index file.
     */
    public int getNumberOfBlocks() {
        return blockOffsets.length;
    }

    /**
     * Get the block offset for a given block.
     */
    public long getPosition(int block) {
        return blockOffsets[block];
    }

    /**
     * Find the next data block start from the given position.
     * Returns NOT_FOUND if it's out of bounds.
     */
    public long findNextPosition(long pos) {
        int block = Arrays.binarySearch(blockOffsets, pos);

        if (block >= 0) { // direct hit on a block start position
            return blockOffsets[block];
        } else {
            block = -block - 1;
            if (block > blockOffsets.length - 1) {
                return NOT_FOUND;
            }
            return blockOffsets[block];
        }
    }

    /**
     * Return the index of block that pos belongs to.
     * Returns NOT_FOUND if it's out of bounds.
     * This helper method can be very useful when indexing data, to avoid storing offset but just block index.
     */
    public long findBelongingBlockIndex(long pos) {
        int block = Arrays.binarySearch(blockOffsets, pos);

        if (block >= 0) { // direct hit
            return block;
        } else {
            block = -block - 1 - 1;
            if (block > (blockOffsets.length - 1) || block<0) {
                return NOT_FOUND;
            }
            return block;
        }
    }


    /**
     * Return true if the index has no blocks set.
     */
    public boolean isEmpty() {
        return blockOffsets == null || blockOffsets.length == 0;
    }

    /**
     * Nudge a given file slice start to the nearest block start no earlier than
     * the current slice start.
     *
     * @param start The current slice start
     * @param end   The current slice end
     * @return The smallest block offset in the index between [start, end), or
     * NOT_FOUND if there is none such.
     */
    public long alignSliceStartToIndex(long start, long end) {
        if (start != 0) {
            // find the next block position from
            // the start of the split
            long newStart = findNextPosition(start);
            if (newStart == NOT_FOUND || newStart >= end) {
                return NOT_FOUND;
            }
            start = newStart;
        }
        return start;
    }

    /**
     * Nudge a given file slice end to the nearest compressed block end no earlier than
     * the current slice end.
     *
     * @param end      The current slice end
     * @param fileSize The size of the file, i.e. the max end position.
     * @return The smallest block offset in the index between [end, fileSize].
     */
    public long alignSliceEndToIndex(long end, long fileSize) {
        long newEnd = findNextPosition(end);
        if (newEnd != NOT_FOUND) {
            end = newEnd;
        } else {
            // didn't find the next position
            // we have hit the end of the file
            end = fileSize;
        }
        return end;
    }

    /**
     * Read the FourMcBlockIndex of the 4mc file.
     * @param fs   The index file is on this file system.
     * @param file the file whose index we are reading
     * @throws java.io.IOException
     */
    public static FourMcBlockIndex readIndex(FileSystem fs, Path file) throws IOException {
        return FourMcInputStream.readIndex(fs, file);
    }

}


