package com.fing.fourmc.spark.protobuf;

import com.fing.compression.fourmc.FourMzHighCodec;
import com.fing.fourmc.elephantbird.adapter.FourMzEbProtoInputFormat;
import com.fing.fourmc.elephantbird.adapter.FourMzEbProtoOutputFormat;
import com.fing.fourmc.protobuf.USER;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.launcher.SparkLauncher;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shows how to run spark job using in input a 4mc EB encoded input.
 * Make sure hadoop-4mc is in spark classpath, or in other case you
 * can use SparkLauncher.addJar to add it to tour run context.
 *
 * The example processes input files in Elephant-Bird 4mz format of USER.User message.
 * Leveraging spark it calculates top tags aggregating them and then saving to an output
 * text file, encoded with 4mz as well.
 */
public class TestProtobufInput {


    public static void main(String[] args) throws IOException, InterruptedException, ParseException {

        JavaSparkContext sc;
        SparkEnv sparkEnv;
        Job jobConf;

        System.out.println("Starting SparkExampleApp...");

        // Spark configuration
        //String master = "local[*]";
        String master = System.getProperty(SparkLauncher.SPARK_MASTER);
        SparkConf conf = new SparkConf().setMaster(master);
        conf.setAppName("TestProtobufInput");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.scheduler.mode", "FAIR");
        sc = new JavaSparkContext(conf);
        Configuration baseConfiguration = new Configuration();
        jobConf = Job.getInstance(baseConfiguration);
        FourMzEbProtoInputFormat.setInputFormatClass(USER.User.class, jobConf);
        FourMzEbProtoOutputFormat.setOutputFormatClass(USER.User.class, FourMzHighCodec.class, jobConf);

        FileSystem hdfs = FileSystem.get(baseConfiguration);
        String outputPath = "hdfs:///test-spark-output";
        if(hdfs.exists(new Path(outputPath))) {
            hdfs.delete(new Path(outputPath), true);
        }
        System.out.println("prepared output: " + outputPath);

        List<String> inputFiles = new ArrayList<>();
        for (String arg : args) {
            System.out.println(" Adding input file > " + arg);
            inputFiles.add(arg);
        }

        // create unique RDD from input files
        JavaPairRDD<LongWritable, ProtobufWritable> uniqueRDD = null;
        StringBuilder inputFilesStr = new StringBuilder();
        for (int cont = 0; cont < inputFiles.size(); cont++) {
            if (cont>0) inputFilesStr.append(",");
            inputFilesStr.append(inputFiles.get(cont));
        }

        uniqueRDD = sc.newAPIHadoopFile(inputFilesStr.toString(), FourMzEbProtoInputFormat.class, LongWritable.class,
                ProtobufWritable.class, jobConf.getConfiguration());
        System.out.println("Created unique RDD for input files");

        // process all users, emit all tags with counter (=1)
        JavaPairRDD<String,Long> tags =
                uniqueRDD.flatMapToPair((PairFlatMapFunction<Tuple2<LongWritable, ProtobufWritable>, String, Long>) value -> {

                    USER.User user = (USER.User) value._2().get();
                    if (user.getTagsCount()==0) return Collections.emptyIterator();

                    List<Tuple2<String, Long>> result = new ArrayList<>();

                    for (String tag : user.getTagsList()) {
                        result.add(new Tuple2<>(tag, 1L));
                    }
                    return result.iterator();
                });

        JavaPairRDD<String, Long> reducedTags = tags.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1+v2);

        // swap and sort them by count, then swap back

        JavaPairRDD<Long, String> swapResults =
                reducedTags.mapToPair((PairFunction<Tuple2<String, Long>, Long, String>) val -> val.swap());

        JavaPairRDD<Long, String> sortedSwapped = swapResults.sortByKey(false, 1);

        JavaPairRDD<String, Long> sortedTags =
                sortedSwapped.mapToPair((PairFunction<Tuple2<Long, String>, String, Long>) val -> val.swap());


        sortedTags.saveAsTextFile(outputPath, FourMzHighCodec.class);

        Path filenamePath = new Path(outputPath+"/summary.txt");
        if (hdfs.exists(filenamePath)) {
            hdfs.delete(filenamePath, true);
        }

        sc.stop();

        sc.close();
    }
}
