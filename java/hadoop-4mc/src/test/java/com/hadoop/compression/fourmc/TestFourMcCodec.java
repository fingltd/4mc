package com.hadoop.compression.fourmc;

import java.io.*;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.util.ReflectionUtils;

public class TestFourMcCodec extends TestCase{
    private static final Log LOG=
            LogFactory.getLog(TestFourMcCodec.class);

    private Configuration conf = new Configuration();
    private int count = 10000;
    private int seed = new Random().nextInt();

    public void testZstdCodec() throws IOException {
        assertTrue(FourMcNativeCodeLoader.isNativeCodeLoaded());
        codecTest(conf, seed, count * 10, "com.hadoop.compression.fourmc.ZstdCodec");
    }

    private static void codecTest(Configuration conf, int seed, int count,
                                  String codecClass)
            throws IOException {

        // Create the codec
        CompressionCodec codec = null;
        try {
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
        } catch (ClassNotFoundException cnfe) {
            throw new IOException("Illegal codec!");
        }
        LOG.info("Created a Codec object of type: " + codecClass);

        // Generate data
        DataOutputBuffer data = new DataOutputBuffer();
        RandomDatum.Generator generator = new RandomDatum.Generator(seed);
        for(int i=0; i < count; ++i) {
            generator.next();
            RandomDatum key = generator.getKey();
            RandomDatum value = generator.getValue();

            key.write(data);
            value.write(data);
        }
        DataInputBuffer originalData = new DataInputBuffer();
        DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData));
        originalData.reset(data.getData(), 0, data.getLength());

        LOG.info("Generated " + count + " records");

        // Compress data
        DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
        CompressionOutputStream deflateFilter =
                codec.createOutputStream(compressedDataBuffer);
        DataOutputStream deflateOut =
                new DataOutputStream(new BufferedOutputStream(deflateFilter));
        long now = System.nanoTime();
        deflateOut.write(data.getData(), 0, data.getLength());
        deflateOut.flush();
        deflateFilter.finish();
        //Necessary to close the stream for BZip2 Codec to write its final output.  Flush is not enough.
        deflateOut.close();
        long compressionTime = System.nanoTime() - now;
        LOG.info("Compression took: " + compressionTime / 1000 + "us");
        // Compression ratio should be very small, almost 1 as the input is random int.
        LOG.info("Compression ratio is: " + (data.getLength() * 1.0 / compressedDataBuffer.getLength()));
        LOG.info("Compression speed is: " + (data.getLength() * 1.0 / (compressionTime / 1000.0)));
        LOG.info("Finished compressing data");

        // De-compress data
        DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
        deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
                compressedDataBuffer.getLength());
        CompressionInputStream inflateFilter =
                codec.createInputStream(deCompressedDataBuffer);
        DataInputStream inflateIn =
                new DataInputStream(new BufferedInputStream(inflateFilter));

        // Check
        now = System.nanoTime();
        for(int i=0; i < count; ++i) {
            RandomDatum k1 = new RandomDatum();
            RandomDatum v1 = new RandomDatum();
            k1.readFields(originalIn);
            v1.readFields(originalIn);

            RandomDatum k2 = new RandomDatum();
            RandomDatum v2 = new RandomDatum();
            k2.readFields(inflateIn);
            v2.readFields(inflateIn);
            assertTrue(k1.equals(k2));
            assertTrue(v1.equals(v2));
        }
        // Ensure all data is consumed.
        assertTrue(originalIn.read() == -1);
        assertTrue(inflateIn.read() == -1);
        LOG.info("Checking took " + (System.nanoTime() - now)/1000 + "us");
        LOG.info("SUCCESS! Completed checking " + count + " records");
    }

}
