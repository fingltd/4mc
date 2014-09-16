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
package com.hadoop.mapreduce;

import com.hadoop.compression.fourmc.FourMcBlockIndex;
import com.hadoop.compression.fourmc.FourMcInputFormatUtil;
import com.hadoop.compression.fourmc.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for 4mc compressed files.
 * This is the base class, mainly managing input splits, leveraging 4mc block index.
 *
 * Subclasses must only make sure to provide an implementation of createRecordReader.
 * See {@link com.hadoop.mapreduce.FourMcTextInputFormat} as example reading text files.
 * 
 * <b>Note:</b> unlikely default hadoop, but exactly like the EB version
 * this recursively examines directories for matching files.
 */
public abstract class FourMcInputFormat<K, V> extends FileInputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(FourMcInputFormat.class.getName());

    private final PathFilter hiddenPathFilter = new PathFilter() {
        // avoid hidden files and directories.

        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith(".") &&
                    !name.startsWith("_");
        }
    };

    private final PathFilter visible4mcFilter = new PathFilter() {
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith(".") &&
                    !name.startsWith("_") &&
                    FourMcInputFormatUtil.is4mcFile(name);
        }
    };

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> files = super.listStatus(job);
        List<FileStatus> results = new ArrayList<FileStatus>();
        Configuration conf = HadoopUtils.getConfiguration(job);
        boolean recursive = conf.getBoolean("mapred.input.dir.recursive", false);
        Iterator<FileStatus> it = files.iterator();
        while (it.hasNext()) {
            FileStatus fileStatus = it.next();
            FileSystem fs = fileStatus.getPath().getFileSystem(conf);
            addInputPath(results, fs, fileStatus, recursive);
        }

        LOG.debug("Total 4mc input paths to process: " + results.size());
        return results;
    }

    protected void addInputPath(List<FileStatus> results, FileSystem fs,
                                FileStatus pathStat, boolean recursive) throws IOException {
        Path path = pathStat.getPath();
        if (pathStat.isDir()) {
            if (recursive) {
                for (FileStatus stat : fs.listStatus(path, hiddenPathFilter)) {
                    addInputPath(results, fs, stat, recursive);
                }
            }
        } else if (visible4mcFilter.accept(path)) {
            results.add(pathStat);
        }
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration conf = HadoopUtils.getConfiguration(job);

        List<InputSplit> defaultSplits = super.getSplits(job);
        List<InputSplit> result = new ArrayList<InputSplit>();

        Path prevFile = null;
        FourMcBlockIndex prevIndex = null;

        for (InputSplit genericSplit : defaultSplits) {
            // Load the index.
            FileSplit fileSplit = (FileSplit) genericSplit;
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);

            FourMcBlockIndex index;
            if (file.equals(prevFile)) {
                index = prevIndex;
            } else {
                index = FourMcBlockIndex.readIndex(fs, file);
                prevFile = file;
                prevIndex = index;
            }

            if (index == null) {
                throw new IOException("BlockIndex unreadable for " + file);
            }

            if (index.isEmpty()) { // leave the default split for empty block index
                result.add(fileSplit);
                continue;
            }

            long start = fileSplit.getStart();
            long end = start + fileSplit.getLength();

            long fourMcStart = index.alignSliceStartToIndex(start, end);
            long fourMcEnd = index.alignSliceEndToIndex(end, fs.getFileStatus(file).getLen());

            if (fourMcStart != FourMcBlockIndex.NOT_FOUND && fourMcEnd != FourMcBlockIndex.NOT_FOUND) {
                result.add(new FileSplit(file, fourMcStart, fourMcEnd - fourMcStart, fileSplit.getLocations()));
                LOG.debug("Added 4mc split for " + file + "[start=" + fourMcStart + ", length=" + (fourMcEnd - fourMcStart) + "]");
            }

        }

        return result;
    }
}

