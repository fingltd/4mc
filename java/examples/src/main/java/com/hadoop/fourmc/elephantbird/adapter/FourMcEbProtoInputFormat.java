package com.hadoop.fourmc.elephantbird.adapter;

import com.google.protobuf.Message;
import com.hadoop.mapreduce.FourMcInputFormat;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockRecordReader;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Adapter between Elephant-Bird framework and 4mc: this is able to make EB framework handle input binary protobuf
 * files in 4mc format, seamlessly handling the input split that leverages the internal 4mc block index.
 * Usage:
 *  where you usually set input:
 *      MultiInputFormat.setInputFormatClass(MyProtoMessage.class, job);
 *  replace with:
 *      FourMcEbProtoInputFormat.setInputFormatClass(MyProtoMessage.class, job);
 *  That's it.
 *
 *  Output format doesn't need anything, as if you want to still encode in 4mc,
 *  you can just do the usual:
 *      XyzOutputFormat.setCompressOutput(job, true);
 *      XyzOutputFormat.setOutputCompressorClass(job, FourMcCodec.class);  // or e.g. FourMcMediumCodec.class
 *
 * @param <M> protobuf message to be read.
 */
public class FourMcEbProtoInputFormat<M extends Message> extends FourMcInputFormat<LongWritable, BinaryWritable<M>> {

    private TypeRef<M> typeRef;
    private static String CLASS_CONF_KEY = "elephantbird.class.for.FourMcEbProtoInputFormat";

    public static void setInputFormatClass(Class<?> clazz, Job job) {
        job.setInputFormatClass(FourMcEbProtoInputFormat.class);
        setClassConf(clazz, HadoopCompat.getConfiguration(job));
    }

    public FourMcEbProtoInputFormat() {
    }

    public FourMcEbProtoInputFormat(TypeRef<M> tr) {
        this.typeRef = tr;
    }

    @Override
    public RecordReader<LongWritable, BinaryWritable<M>> createRecordReader(InputSplit split, TaskAttemptContext taskAttempt) {
        Configuration conf = HadoopCompat.getConfiguration(taskAttempt);
        if (typeRef == null) {
            setTypeRef(conf);
        }
        return new LzoProtobufBlockRecordReader(typeRef);
    }

    public static void setClassConf(Class<?> clazz, Configuration conf) {
        HadoopUtils.setClassConf(conf, CLASS_CONF_KEY, clazz);
    }

    private void setTypeRef(Configuration conf) {
        String className = conf.get(CLASS_CONF_KEY);

        if (className == null) {
            throw new RuntimeException(CLASS_CONF_KEY + " is not set");
        }

        Class<?> clazz = null;
        try {
            clazz = conf.getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to instantiate class '" + className + "'", e);
        }

        typeRef = new TypeRef<M>(clazz) {
        };
    }


}
