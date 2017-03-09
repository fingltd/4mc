# 4mc - Four More Compression

## About

The 4MC (4 More Compression) is a library for hadoop providing a new **splittable compressed file format** (4mc) which lets you leverage the power of LZ4 and ZSTD algorithms. It's been designed to add more features to existing big data solutions like HADOOP/ElephantBird, addressing the following major points:
* **Performances**: LZ4 isn't just the world's fastest compression algorithm, it can also achieve seamlessly much higher compression ratios, by using medium/high codecs, reaching ratios very near GZIP. It only costs some compression CPU time, since decompression remains identical, or speed up even more. Fresh born ZSTD achieves even
better compression rates with amazing performances, it's just perfect for long-term big data.
* **Hadoop/EB**: current state-of-the art solution needs external index file to be able to split and process in parallel the big files, leveraging local mappers. 4mc format has been designed for big data purpose, thus the block index is internal, *there is no need for any external file or pre processing of input data*: any 4mc/4mz file is ready for parallel processing.
* **Licensing**: LZ4, ZSTD and 4mc licenses are BSD!

## License

BSD 2-Clause License - http://www.opensource.org/licenses/bsd-license.php

## 4MC package content

4MC is composed by the following items, included in source code repository:
* **hadoop-4mc** - java library to be used with hadoop to leverage 4mc format and LZ4 codecs
* **hadoop-4mc lib** native - JNI bindings leveraging LZ4 compression/decompression (embedded in jar, no need to recompile)
* **4mc** - command line tool for compression/decompression of your files - written in C, working on Linux/MacOS/Windows (available in tool folder)

## Compression speed and levels

4mc comes with 4 compression levels and 2 compression algorithms: 4mc format leverages the LZ4 standard library, while 4mz format leverages ZSTD library. Both 4mc command line tool and Java HADOOP classes do provide codecs for these 4 levels.
* **4mc Fast (LZ4)** Compression: default one, using LZ4 fast
* **4mc Medium (LZ4)** Compression: LZ4 MC
* **4mc High (LZ4)** Compression: LZ4 HC lvl 4
* **4mc Ultra (LZ4)** Compression: LZ4 HC lvl 8
* **4mz Fast (zstd)** Compression: ZSTD lvl 1
* **4mz Medium (zstd)** Compression: ZSTD lvl 3
* **4mz High (zstd)** Compression: ZSTD lvl 6
* **4mz Ultra (zstd)** Compression: ZSTD lvl 12

Bechmark with silesia on MacOS OSX El Captain - Intel(R) CPU 64bit @ 2.5GHz Core i7
```
 Algorithm      Compression Speed     Decompression Speed      Ratio
 ZSTD-Fast               225 MB/s                330 MB/s      2.873
 ZSTD-Medium             140 MB/s                301 MB/s      3.151
 ZSTD-High                62 MB/s                307 MB/s      3.341
 ZSTD-Ultra               16 MB/s                326 MB/s      3.529
 LZ4-Fast                270 MB/s                460 MB/s      2.084
 LZ4-Medium              135 MB/s                460 MB/s      2.340
 LZ4-High                 57 MB/s                495 MB/s      2.630
 LZ4-Ultra                31 MB/s                502 MB/s      2.716
```
Please note that 4mc/4mz compression codecs can be also used in any stage of the M/R as compression codecs.
ZSTD is winning over LZ4 on almost all use cases, except for super real-time cases or near real-time cases where
you are not needing long-term storage.


## Releases and change history
Releases with artifacts available at https://github.com/carlomedas/4mc/releases - Attached artifacts contain jar with embedded native library for Windows/Linux/MacOS. You can anyway compile JNI bindings for your own platform and override embedded ones.
4mc CLI tool for all platforms is now available at https://github.com/carlomedas/4mc/tree/master/tool
* **4mc 2.0.0** - 4mz to support ZSTD (zstandard https://github.com/facebook/zstd)
* **4mc 1.4.0** - Native libraries are now embedded in jar, thus hadoop-4mc library can be used w/o manual configurations on Hadoop/Spark/Flink/etc
* **4mc 1.3.0** - Introduced direct buffers pool, to cope with "java.lang.OufOfMemoryError: Direct Buffer Memory"
* **4mc 1.1.0** - Support both of hadoop-1 and hadoop-2
* **4mc 1.0.0** - Very first version of 4mc

## Build

* **Native:** 4mc command line tool and hadoop-4mc native library for JNI codecs
  Makefile is provided for unix/linux/mac; also cmake can be used (best choice on Windows).

* **Java:** hadoop-4mc library for hadoop can be built with maven, using provided pom.
* **Java Native:** see above, make sure JAVA_HOME is set.

## Hadoop configuration

You only have to make sure that your jobs depends on hadoop-4mc jar and they bring it and set it as shared lib needed for cluster execution.
Enabling codecs has no difference from usual, i.e. by adding them to configuration xml (core-site.xml):
```
	<property>
        <name>io.compression.codecs</name>
        <value>
			<!-- standard and lzo codecs -->
			org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,
			com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,
			<!-- 4mc codecs -->
			com.hadoop.compression.fourmc.Lz4Codec,com.hadoop.compression.fourmc.Lz4MediumCodec,com.hadoop.compression.fourmc.Lz4HighCodec,com.hadoop.compression.fourmc.Lz4UltraCodec,
			com.hadoop.compression.fourmc.FourMcCodec,com.hadoop.compression.fourmc.FourMcMediumCodec,com.hadoop.compression.fourmc.FourMcHighCodec,com.hadoop.compression.fourmc.FourMcUltraCodec,
      <!-- 4mz codecs -->
      com.hadoop.compression.fourmc.FourMzCodec,com.hadoop.compression.fourmc.FourMzMediumCodec,com.hadoop.compression.fourmc.FourMzHighCodec,com.hadoop.compression.fourmc.FourMzUltraCodec
		</value>
    </property>
```

Please note that snippet above enables all codecs provided in the library, as follows:
* **4mc codecs** to read and write splittable LZ4 compressed files: *FourMcCodec FourMcMediumCodec FourMcHighCodec FourMcUltraCodec*
* **4mz codecs** to read and write splittable ZSTD compressed files: *FourMzCodec FourMzMediumCodec FourMzHighCodec FourMzUltraCodec*
* **straight LZ4 codecs** usable in your intermediate job outputs or as alternate compression for your solution (e.g. in SequenceFile): *Lz4Codec Lz4MediumCodec Lz4HighCodec Lz4UltraCodec*
* **straight ZSTD codecs** usable in your intermediate job outputs or as alternate compression for your solution (e.g. in SequenceFile): *ZstdCodec ZstdMediumCodec ZstdHighCodec ZstdUltraCodec*

*Why so many different codecs and not usual single one reading level from config?*
The aim here is to have by all means a way to programmatically tune your M/R engine at any stage.
E.g. use case: M/R job willing to have a fast/medium codec as intermediate map output, and then high codec in output, as data is going to be kept for long time.
Please remember once again that compression level in both ZSTD and LZ4 is seamless to the decompressor and the more you compress the data not only affects the output size but also the decompressor speed, as it gets even faster.

## Java examples

The maven module **examples** is a separate module providing several usage examples with hadoop Map/Reduce and
also with Spark. Flink examples will be added soon, but it's straightforward like Spark.
As you can see in the examples, 4mc can be used with text input/output but also it can leverge **ElephantBird**
framework to process protobuf encoded binary data.

## PySpark Example

Use `sc.newAPIHadoopFile` to load your data. This will leverage the splittable feature of 4mc and load your data into many partitions.

```python
filepath = 'gs://data/foo.4mc'

# This will read the file and partition it as it loads
data = sc.newAPIHadoopFile(
    filepath
,   'com.hadoop.mapreduce.FourMcTextInputFormat'
,   'org.apache.hadoop.io.LongWritable'
,   'org.apache.hadoop.io.Text'
)
data.getNumPartitions()
# -> 24

# This is what the RDD looks like after it's loaded
data.take(1)
# -> [(0, 'first line')]
```

You may use `sc.textFile` or any other method to load the data. However, the data will be loaded in one partition only.

```python
data = sc.textFile(filepath)
data.getNumPartitions()
# -> 1
data.take(1)
```


## How To Contribute

Bug fixes, features, and documentation improvements are welcome!

## Contributors

Major contributors are listed below.

* Carlo Medas - *author of the 4mc format and library*
* Yann Collet - *mentor, author of LZ4 and ZSTD compression libraries*
