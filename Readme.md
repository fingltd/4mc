# 4mc - Four More Compression

## About

The 4MC (4 More Compression) is a library for hadoop providing a new **splittable compressed file format** (4mc) which lets you leverage the power of LZ4 algorithm. It's been designed to add more features to existing big data solutions like HADOOP/ElephantBird, addressing the following major points:
* **Performances**: LZ4 isn't just the world's fastest compression algorithm, it can also achieve seamlessly much higher compression ratios, by using medium/high codecs, reaching ratios very near GZIP. It only costs some compression CPU time, since decompression remains identical, or speed up even more.
* **Hadoop/EB**: current state-of-the art solution needs external index file to be able to split and process in parallel the big files, leveraging local mappers. 4mc format has been designed for big data purpose, thus the block index is internal, *there is no need for any external file or pre processing of input data*: any 4mc file is ready for parallel processing.
* **Licensing**: LZ4 and 4mc licenses are BSD!

## License

BSD 2-Clause License - http://www.opensource.org/licenses/bsd-license.php

## 4MC package content

4MC is composed by the following items, included in source code repository:
* **hadoop-4mc** - java library to be used with hadoop to leverage 4mc format and LZ4 codecs (needs JNI below but in latest version the native library is embedded in jar)
* **hadoop-4mc lib** native - JNI bindings leveraging LZ4 compression/decompression
* **4mc** - command line tool for compression/decompression of your files - written in C, working on Linux/MacOS/Windows

## Compression speed and levels

4mc comes with 4 compression levels, all of them leveraging the LZ4 standard library. Both 4mc command line tool and Java HADOOP classes do provide codecs for these 4 levels.
* **Fast** Compression: default one reaching up to 500 MB/s (LZ4 fast)
* **Medium** Compression: half speed of fast mode, +13% ratio (LZ4 MC)
* **High** Compression: 5x slower than fast, +25% ratio (LZ4 HC lvl 4)
* **Ultra** Compression: 13x slower than fast, +30% ratio (LZ4 HC lvl 8) 

Bechmark with silesia on Linux CentOS 6.4 64bit - Intel(R) CPU 64bit @ 2.70GHz
```
 Algorithm     Compression Speed     Decompression Speed      Ratio
 Fast                   390 MB/s               2200 MB/s      2.084
 Medium                 170 MB/s               2206 MB/s      2.340
 High                    69 MB/s               2436 MB/s      2.630
 Ultra                   30 MB/s               2515 MB/s      2.716
```
Please note that 4mc compression codecs can be also used in any stage of the M/R as compression codecs.

## Releases and change history
Releases with artifacts available at https://github.com/carlomedas/4mc/releases - Attached artifacts contain jar with embedded native library for Windows/Linux/MacOS. You can anyway compile JNI bindings for your own platform and override embedded ones.
4mc CLI tool for all platforms is now available at https://github.com/carlomedas/4mc/tree/master/tool
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

First of all you have to copy hadoop-4mc jar and related native to your cluster lib path and native lib path respectively.
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
			com.hadoop.compression.fourmc.FourMcCodec,com.hadoop.compression.fourmc.FourMcMediumCodec,com.hadoop.compression.fourmc.FourMcHighCodec,com.hadoop.compression.fourmc.FourMcUltraCodec
		</value>
    </property>
```

Please note that snippet above enables all codecs provided in the library, as follows:
* **4mc codecs** to read and write splittable LZ4 compressed files: *FourMcCodec FourMcMediumCodec FourMcHighCodec FourMcUltraCodec*
* **straight LZ4 codecs** usable in your intermediate job outputs or as alternate compression for your solution (e.g. in SequenceFile): *Lz4Codec Lz4MediumCodec Lz4HighCodec Lz4UltraCodec*

*Why so many different codecs and not usual single one reading level from config?*
The aim here is to have by all means a way to programmatically tune your M/R engine at any stage.
E.g. use case: M/R job willing to have a fast/medium codec as intermediate map output, and then high codec in output, as data is going to be kept for long time.
Please remember once again that compression level in LZ4 is seamless to the decompressor and the more you compress the data not only affects the output size but also the decompressor speed, as it gets even faster.

## Java examples

In java examples folder you can find 2 examples:
* **text/TestTextInput.java** : this example is a perfect skeleton to start working with 4mc files on your text files
* **elephant-bird/FourMcEbProtoInputFormat.java** : this adapter bridges EB frameworking letting you read 4mc files containing binary protobuf objects of your own type
* **elephant-bird/FourMcEbProtoOutputFormat.java** : this adapter bridges EB frameworking letting you write 4mc files containing binary protobuf objects of your own type

## How To Contribute

This is the very first version of the library. Bug fixes, features, and documentation improvements are welcome!

## Contributors

Major contributors are listed below.

* Carlo Medas - *author of the 4mc format and library*
* Yann Collet - *mentor, author of LZ4 compression library*
