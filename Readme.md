# 4mc - Four More Compression

## About

The 4MC (4 More Compression) is a library for hadoop providing a new **splittable compressed file format** (4mc) which lets you leverage the power of LZ4 algorithm. It's been designed to add more features to existing big data solutions like HADOOP/ElephantBird which are currently mainly based on LZO, addressing the following major points:
* **Licensing**: LZO is GPL, while LZ4 and 4mc are BSD!
* **Performances**: LZ4 is world's fastest compression but not only! It can achieve seamlessly much higher compression ratios by using medium/high codecs, thus going very near to GZIP ratios, at the price of compression CPU time only, since decompressors are simply going to speed up even more. 
* **Hadoop/EB**: hadoop-lzo solution needs external index file to be able to split and process in parallel the big files, leveraging local mappers. 4mc format has been designed for big data purpose, thus the block index is internal, there is no need for any external file or pre processing of input data: any 4mc file is ready for parallel processing.

## License

BSD 2-Clause License - http://www.opensource.org/licenses/bsd-license.php

## 4MC package content

4MC is composed by the following items, included in source code repository:
* **4mc** - command line tool for compression/decompression of your files - written in C, working on Linux/MacOS/Windows
* **hadoop-4mc** - java library to be used with hadoop to leverage 4mc format and LZ4 codecs (needs JNI below)
* **hadoop-4mc lib** native - JNI bindings leveraging LZ4 compression/decompression

## Compression speed and levels

4mc comes with 4 compression levels, all of them leveraging the LZ4 standard library. Both 4mc command line tool and Java HADOOP classes do provide codecs for these 4 levels.
* **Fast** Compression: default one reaching up to 500 MB/s (LZ4 fast)
* **Medium** Compression: half speed of fast mode, +13% ratio (LZ4 MC)
* **High** Compression: 5x slower than fast, +25% ratio (LZ4 HC lvl 4)
* **Ultra** Compression: 13x slower than fast, +30% ratio (LZ4 HC lvl 8) 

Bechmark with silesia on Linux CentOS 6.4 64bit - HP DL 380P Intel(R) Xeon(R) CPU E5-2697 v2 @ 2.70GHz
```
 Algorithm     Compression Speed     Decompression Speed      Ratio
 Fast                   390 MB/s               2200 MB/s      2.084
 Medium                 170 MB/s               2206 MB/s      2.340
 High                    69 MB/s               2436 MB/s      2.630
 Ultra                   30 MB/s               2515 MB/s      2.716
```
Please note that 4mc compression codecs can be also used in any stage of the M/R as compression codecs.

## Releases and change history
4mc 1.0.0 - https://github.com/carlomedas/4mc/releases
Very first version of 4mc (attached artifacts contain jar and 4mc.exe for Windows, 4mc CLI and JNI better to compile for your own platform).

Hadoop-1.x : working good.
Hadoop-2.x : test in progress.

## Build

* **Native:** 4mc command line tool and hadoop-4mc native library for JNI codecs
  Makefile is provided for unix/linux; also cmake can be used (still needs refinement for Windows).

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

## How To Contribute

This is the very first version of the library. Bug fixes, features, and documentation improvements are welcome!

## Contributors

Major contributors are listed below.

* Carlo Medas - *author of the mc4 format and library*
* Yann Collett - *mentor, author of LZ4 compression library*
