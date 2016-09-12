package com.hadoop.fourmc.protobuf.lz4;

import com.hadoop.compression.fourmc.*;
import com.hadoop.fourmc.elephantbird.adapter.FourMcEbProtoInputFormat;
import com.hadoop.fourmc.elephantbird.adapter.FourMcEbProtoOutputFormat;
import com.hadoop.fourmc.protobuf.USER;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Tests hadoop-4mc (LZ4, FourMc family) with Elephant Bird encoded 4mc: both input and output are encoded.
 * Command line arguments:
 *    input file/folder
 *    output folder
 *    fast|medium|high|ultra|plain   changes the output codec for compression and performance tuning
 */
public class TestProtobufIO {

    public static class TestMapper extends Mapper<LongWritable, ProtobufWritable<USER.User>, Text, ProtobufWritable<USER.User>> {


        @Override
        protected void map(LongWritable key, ProtobufWritable<USER.User> value, Context context) throws IOException, InterruptedException {
            // this is just an identity mapper but here you can manipulate or creare other proto objects.
            USER.User user = value.get();
            if (user.hasName()) {
                context.write(null, value);
            }
        }
    }


    public int runTest(String[] args, Configuration conf) throws Exception {

        Job job = new Job(conf);
        job.setJobName("4mc.TestProtobufIO");

        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);

        FourMcEbProtoInputFormat.setInputFormatClass(USER.User.class, job);

        if (args[2].equals("fast")) {
            FourMcEbProtoOutputFormat.setOutputFormatClass(USER.User.class, FourMcCodec.class, job);
            System.out.println("Output: 4MC FAST");
        } else if (args[2].equals("medium")) {
            FourMcEbProtoOutputFormat.setOutputFormatClass(USER.User.class, FourMcMediumCodec.class, job);
            System.out.println("Output: 4MC MEDIUM");
        } else if (args[2].equals("high")) {
            FourMcEbProtoOutputFormat.setOutputFormatClass(USER.User.class, FourMcHighCodec.class, job);
            System.out.println("Output: 4MC HIGH");
        } else if (args[2].equals("ultra")) {
            FourMcEbProtoOutputFormat.setOutputFormatClass(USER.User.class, FourMcUltraCodec.class, job);
            System.out.println("Output: 4MC ULTRA");
        } else {
            System.out.println("Output: PLAIN");
        }


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //args = new GenericOptionsParser(conf, args).getRemainingArgs();
        TestProtobufIO runner = new TestProtobufIO();

        if (args.length < 3) {
            System.out.println(
                    "Usage: hadoop jar path/to/this.jar " + runner.getClass() +
                            " <input file/folder> <output dir> <fast|medium|high|ultra>");
            System.exit(1);
        }

        System.exit(runner.runTest(args, conf));
    }

}
