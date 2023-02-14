package com.fing.fourmc.mapreduce.text.lz4;

import com.fing.compression.fourmc.FourMcCodec;
import com.fing.compression.fourmc.FourMcHighCodec;
import com.fing.compression.fourmc.FourMcMediumCodec;
import com.fing.compression.fourmc.FourMcUltraCodec;
import com.fing.mapreduce.FourMcTextInputFormat;
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
 * Tests hadoop-4mc (LZ4, FourMc family) with identity Mappers jobs that just replicate the input file(s).
 * Command line arguments:
 *    input file/folder
 *    output folder
 *    fast|medium|high|ultra|plain   changes the output codec for compression and performance tuning
 */
public class TestTextInput {

    public static class TestMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text emptyText = new Text();
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(null, value);
        }
    }


    public int runTest(String[] args, Configuration conf) throws Exception {
        //Path inputPath = new Path(args[0]);
        //Path outputPath = new Path(args[1]);

        Job job = new Job(conf);
        job.setJobName("4mc.TestTextInput");

        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(FourMcTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (args[2].equals("fast")) {
            TextOutputFormat.setCompressOutput(job, true);
            TextOutputFormat.setOutputCompressorClass(job, FourMcCodec.class);
            System.out.println("Output: 4MC FAST");
        } else if (args[2].equals("medium")) {
            TextOutputFormat.setCompressOutput(job, true);
            TextOutputFormat.setOutputCompressorClass(job, FourMcMediumCodec.class);
            System.out.println("Output: 4MC MEDIUM");
        } else if (args[2].equals("high")) {
            TextOutputFormat.setCompressOutput(job, true);
            TextOutputFormat.setOutputCompressorClass(job, FourMcHighCodec.class);
            System.out.println("Output: 4MC HIGH");
        } else if (args[2].equals("ultra")) {
            TextOutputFormat.setCompressOutput(job, true);
            TextOutputFormat.setOutputCompressorClass(job, FourMcUltraCodec.class);
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
        TestTextInput runner = new TestTextInput();

        if (args.length < 3) {
            System.out.println(
                    "Usage: hadoop jar path/to/this.jar " + runner.getClass() +
                            " <input file/folder> <output dir> <fast|medium|high|ultra>");
            System.exit(1);
        }

        System.exit(runner.runTest(args, conf));
    }

}
