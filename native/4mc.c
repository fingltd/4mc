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

#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  define _CRT_SECURE_NO_WARNINGS
#  define _CRT_SECURE_NO_DEPRECATE     // VS2005
#  pragma warning(disable : 4127)      // disable: C4127: conditional expression is constant
#else
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif

#define _FILE_OFFSET_BITS 64   // Large file support on 32-bits unix
#define _POSIX_SOURCE 1        // for fileno() within <stdio.h> on unix


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "lz4/lz4.h"
#include "lz4/lz4hc.h"
#include "lz4/lz4mc.h"
#include "lz4/xxhash.h"

#include "zstd/zstd.h"

#include "4mc.h"


#if defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(_WIN32) || defined(__CYGWIN__)
#  include <fcntl.h>    // _O_BINARY
#  include <io.h>       // _setmode, _isatty
#  ifdef __MINGW32__
   //int _fileno(FILE *stream);   // MINGW somehow forgets to include this windows declaration into <stdio.h>
#  endif
#  define SET_BINARY_MODE(file) _setmode(_fileno(file), _O_BINARY)
#  define IS_CONSOLE(stdStream) _isatty(_fileno(stdStream))
#else
#  include <unistd.h>   // isatty
#  define SET_BINARY_MODE(file)
#  define IS_CONSOLE(stdStream) isatty(fileno(stdStream))
#endif


#define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)

#if defined(_MSC_VER)    // Visual Studio
#  define swap32 _byteswap_ulong
#elif GCC_VERSION >= 403
#  define swap32 __builtin_bswap32
#else
static inline unsigned int swap32(unsigned int x)
{
    return ((x << 24) & 0xff000000 ) |
           ((x <<  8) & 0x00ff0000 ) |
           ((x >>  8) & 0x0000ff00 ) |
           ((x >> 24) & 0x000000ff );
}
#endif



#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)

#define _1BIT  0x01
#define _2BITS 0x03
#define _3BITS 0x07
#define _4BITS 0x0F
#define _8BITS 0xFF

#define CACHELINE 64
#define FOURMC_MAGICNUMBER  0x344D4300
#define FOURMZ_MAGICNUMBER  0x344D5A00
#define FOURMC_VERSION      1
#define MAGICNUMBER_SIZE    4
#define FOURMC_HEADERSIZE   (MAGICNUMBER_SIZE+8)
#define FOURMC_BLOCKSIZE    (4*1024*1024)
#define FOURMC_FOOTERSIZE(blocks) (8+4*blocks+4+MAGICNUMBER_SIZE+4)

static const int one = 1;
#define CPU_LITTLE_ENDIAN   (*(char*)(&one))
#define CPU_BIG_ENDIAN      (!CPU_LITTLE_ENDIAN)
#define LITTLE_ENDIAN_32(i) (CPU_LITTLE_ENDIAN?(i):swap32(i))
#define BIG_ENDIAN_32(i)    (CPU_LITTLE_ENDIAN?(unsigned int)swap32(i):(i))
#define BIG_ENDIAN_64(i)    (CPU_LITTLE_ENDIAN?(swap32((unsigned int)(i>>32))|swap32((unsigned int)(i))):(i))


#define CONSOLE_PRINT(...)         fprintf(stderr, __VA_ARGS__)
#define CONSOLE_PRINT_LEVEL(l, ...) if (displayLevel>=l) { CONSOLE_PRINT(__VA_ARGS__); }



//**************************************
// Exceptions
//**************************************
#define EXIT_WITH_FATALERROR(...)                                         \
{                                                                         \
    CONSOLE_PRINT_LEVEL(1, __VA_ARGS__);                                         \
    CONSOLE_PRINT_LEVEL(1, "\n");                                                \
    exit(1);                                                              \
}

#define EXIT_WITH_FATALERROR_INPUT(...)                                         \
{                                                                         \
    CONSOLE_PRINT_LEVEL(1, __VA_ARGS__);                                         \
    CONSOLE_PRINT_LEVEL(1, "\n");                                                \
    exit(2);                                                              \
}

#define EXIT_WITH_FATALERROR_OUTPUT(...)                                         \
{                                                                         \
    CONSOLE_PRINT_LEVEL(1, __VA_ARGS__);                                         \
    CONSOLE_PRINT_LEVEL(1, "\n");                                                \
    exit(3);                                                              \
}

#define EXIT_WITH_FATALERROR_CONTENT(...)                                         \
{                                                                         \
    CONSOLE_PRINT_LEVEL(1, __VA_ARGS__);                                         \
    CONSOLE_PRINT_LEVEL(1, "\n");                                                \
    exit(4);                                                              \
}


static int openIOFileHandles(int displayLevel, int overwrite, char* input_filename, char* output_filename, FILE** pfinput, FILE** pfoutput)
{

    if (!strcmp (input_filename, stdinmark))
    {
        CONSOLE_PRINT_LEVEL(4,"Using stdin for input\n");
        *pfinput = stdin;
        SET_BINARY_MODE(stdin);
    }
    else
    {
        *pfinput = fopen(input_filename, "rb");
    }

    if (!strcmp (output_filename, stdoutmark))
    {
        CONSOLE_PRINT_LEVEL(4,"Using stdout for output\n");
        *pfoutput = stdout;
        SET_BINARY_MODE(stdout);
    }
    else
    {
        // Check if destination file already exists
        *pfoutput=0;
        if (output_filename != nulmark) *pfoutput = fopen( output_filename, "rb" );
        if (*pfoutput!=0)
        {
            fclose(*pfoutput);
            if (!overwrite)
            {
                char ch;
                CONSOLE_PRINT_LEVEL(2, "Warning : %s already exists\n", output_filename);
                CONSOLE_PRINT_LEVEL(2, "Overwrite ? (Y/N) : ");
                if (displayLevel <= 1) EXIT_WITH_FATALERROR_OUTPUT("Operation aborted : %s already exists", output_filename);   // No interaction possible
                ch = (char)getchar();
                if ((ch!='Y') && (ch!='y')) EXIT_WITH_FATALERROR_OUTPUT("Operation aborted : %s already exists", output_filename);
            }
        }
        *pfoutput = fopen( output_filename, "wb" );
    }

    if ( *pfinput==0 ) EXIT_WITH_FATALERROR_INPUT("Cannot open input file: %s", input_filename);
    if ( *pfoutput==0) EXIT_WITH_FATALERROR_OUTPUT("Cannot open output file: %s", output_filename);

    return 0;
}


FORCE_INLINE int LZ4_compress_default_local(const char* src, char* dst, int size, int maxOut, int clevel)
{ (void)clevel; return LZ4_compress_default(src, dst, size, maxOut); }


FORCE_INLINE int LZ4_compressMC_limitedOutput_local(const char* src, char* dst, int size, int maxOut, int clevel)
{ (void)clevel; return LZ4_compressMC_limitedOutput(src, dst, size, maxOut); }


int fourMCcompressFilename(int displayLevel, int overwrite, char* input_filename, char* output_filename, int compressionLevel)
{
    int (*compressionFunction)(const char*, char*, int, int, int);
    int lz4Level=1; unsigned int bi;
    unsigned long long filesize = 0;
    unsigned long long compressedfilesize = 0;
    unsigned int checkbits;
    char* in_buff;
    char* out_buff;
    char* headerBuffer;
    FILE* finput;
    FILE* foutput;
    clock_t start, end;
    size_t sizeCheck, header_size, readSize;

    unsigned int blockIndexesCount=0;
    unsigned int blockIndexesReserved=8;
    unsigned long long* blockIndexes = (unsigned long long*)calloc(blockIndexesReserved,sizeof(unsigned long long));

    // Init
    start = clock();
    if ((displayLevel==2) && (compressionLevel>1)) displayLevel=3;

    if (compressionLevel <= 1) {
        compressionFunction = LZ4_compress_default_local;
    } else if (compressionLevel == 2) {
        compressionFunction = LZ4_compressMC_limitedOutput_local;
    } else if (compressionLevel == 3) {
        compressionFunction = LZ4_compress_HC;
        lz4Level=4;
    } else {
        compressionFunction = LZ4_compress_HC;
        lz4Level=8;
    }

    openIOFileHandles(displayLevel, overwrite, input_filename, output_filename, &finput, &foutput);

    // Allocate Memory
    in_buff  = (char*)malloc(FOURMC_BLOCKSIZE);
    out_buff = (char*)malloc(FOURMC_BLOCKSIZE+CACHELINE);
    headerBuffer = (char*)malloc(FOURMC_HEADERSIZE);
    if (!in_buff || !out_buff || !(headerBuffer)) EXIT_WITH_FATALERROR("Allocation error : not enough memory");

    // Write Archive Header
    *(unsigned int*)headerBuffer = BIG_ENDIAN_32(FOURMC_MAGICNUMBER);
    *(unsigned int*)(headerBuffer+4)  = BIG_ENDIAN_32(FOURMC_VERSION);

    checkbits = XXH32((headerBuffer), 8, 0);
    *(unsigned int*)(headerBuffer+8)  = BIG_ENDIAN_32(checkbits);
    header_size = FOURMC_HEADERSIZE;

    // Write header
    sizeCheck = fwrite(headerBuffer, 1, header_size, foutput);
    if (sizeCheck!=header_size) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write header");
    compressedfilesize += header_size;

    // read first block
    readSize = fread(in_buff, (size_t)1, (size_t)FOURMC_BLOCKSIZE, finput);

    // Main Loop
    while (readSize>0)
    {
        unsigned int outSize;

        // ------------------------------
        if (++blockIndexesCount >= blockIndexesReserved) {
            unsigned int curSize = blockIndexesReserved;
            unsigned long long* newBlockIndexes = (unsigned long long*)calloc(blockIndexesReserved*2,sizeof(unsigned long long));
            blockIndexesReserved*=2;
            for (bi=0; bi<curSize; ++bi) newBlockIndexes[bi] = blockIndexes[bi];
            free(blockIndexes);
            blockIndexes = newBlockIndexes;
        }
        blockIndexes[blockIndexesCount-1] = compressedfilesize;
        // ------------------------------

        filesize += readSize;
        CONSOLE_PRINT_LEVEL(3, "\rRead : %i MB   ", (int)(filesize>>20));


        // Compress Block
        outSize = compressionFunction(in_buff, out_buff+12, (int)readSize, (int)readSize-1, lz4Level);
        if (outSize > 0) compressedfilesize += outSize+12; else compressedfilesize += readSize+12;
        CONSOLE_PRINT_LEVEL(3, "==> %.2f%%   ", (double)compressedfilesize/filesize*100);

        // Write Block
        if (outSize > 0)
        {
            int sizeToWrite; unsigned int checksum;
            *(unsigned int*)(out_buff) = BIG_ENDIAN_32(readSize);
            *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(outSize);
            checksum = XXH32(out_buff+12, outSize, 0);
            *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(checksum);

            sizeToWrite = 12 + outSize;
            sizeCheck = fwrite(out_buff, 1, sizeToWrite, foutput);
            if (sizeCheck!=(size_t)(sizeToWrite)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write compressed block");
        }
        else  // Copy Original Uncompressed
        {
            unsigned int checksum;
            *(unsigned int*)(out_buff) = BIG_ENDIAN_32(readSize);
            *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(readSize);
            checksum = XXH32(in_buff, (int)readSize, 0);
            *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(checksum);
            sizeCheck = fwrite(out_buff, 1, 12, foutput);
            if (sizeCheck!=(size_t)(12)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write block header");
            sizeCheck = fwrite(in_buff, 1, readSize, foutput);
            if (sizeCheck!=readSize) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write block");
        }

        // Read next block
        readSize = fread(in_buff, (size_t)1, (size_t)FOURMC_BLOCKSIZE, finput);
    }

    // >>>> End of Stream mark <<<<
    *(unsigned int*)(out_buff) = BIG_ENDIAN_32(0);
    *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(0);
    *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(0);
    sizeCheck = fwrite(out_buff, 1, 12, foutput);
    if (sizeCheck!=(size_t)(12)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write end of stream");
    compressedfilesize += 12;

    // >>>> FOOTER <<<<
    free(headerBuffer);
    header_size = FOURMC_FOOTERSIZE(blockIndexesCount);
    headerBuffer = (char*)malloc(header_size);
    *(unsigned int*)(headerBuffer+0) = BIG_ENDIAN_32((unsigned int)header_size); // footer size
    *(unsigned int*)(headerBuffer+4) = BIG_ENDIAN_32(1); // version
    for (bi=0; bi<blockIndexesCount; ++bi) {
        unsigned int blockDelta;
        blockDelta = (unsigned int)(bi==0 ? ( blockIndexes[bi] ) : (blockIndexes[bi] - blockIndexes[bi-1]));
        *(unsigned long long*)(headerBuffer+8+(bi*4)) = BIG_ENDIAN_32(blockDelta);
        CONSOLE_PRINT_LEVEL(4, " * Block #%u at delta %u\n", bi, blockDelta);
    }
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4))   = BIG_ENDIAN_32((unsigned int)header_size); // footer size
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4)+4) = BIG_ENDIAN_32(FOURMC_MAGICNUMBER); // footer size
    checkbits = XXH32(headerBuffer, header_size-4, 0);
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4)+8) = BIG_ENDIAN_32(checkbits); // footer size

    sizeCheck = fwrite(headerBuffer, 1, header_size, foutput);
    if (sizeCheck!=(size_t)(header_size)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write end of stream");
    compressedfilesize += header_size;

    // Close & Free
    free(in_buff);
    free(out_buff);
    free(headerBuffer);
    fclose(finput);
    fclose(foutput);
    free(blockIndexes);

    // Final Status
    end = clock();
    CONSOLE_PRINT_LEVEL(2, "\r%79s\r", "");
    CONSOLE_PRINT_LEVEL(2, "Compressed (%s) %llu bytes into %llu bytes ==> %.2f%% (Ratio=%.3f)\n",
                        compressionLevel<=1?"fast":(compressionLevel==2?"medium":(compressionLevel==3?"high":"ultra")),
                        (unsigned long long) filesize, (unsigned long long) compressedfilesize, (double)compressedfilesize/filesize*100,
                        (double)100.0/((double)compressedfilesize/filesize*100));

    {
        double seconds = (double)(end - start)/CLOCKS_PER_SEC;
        CONSOLE_PRINT_LEVEL(4, "Done in %.2f s ==> %.2f MB/s\n", seconds, (double)filesize / seconds / 1024 / 1024);
    }

    return 0;
}


int fourMZcompressFilename(int displayLevel, int overwrite, char* input_filename, char* output_filename, int compressionLevel)
{
    int zstdLevel=3; unsigned int bi;
    unsigned long long filesize = 0;
    unsigned long long compressedfilesize = 0;
    unsigned int checkbits;
    char* in_buff;
    char* out_buff;
    char* headerBuffer;
    FILE* finput;
    FILE* foutput;
    clock_t start, end;
    size_t sizeCheck, header_size, readSize;

    unsigned int blockIndexesCount=0;
    unsigned int blockIndexesReserved=8;
    unsigned long long* blockIndexes = (unsigned long long*)calloc(blockIndexesReserved,sizeof(unsigned long long));

    // Init
    start = clock();
    if ((displayLevel==2) && (compressionLevel>1)) displayLevel=3;

    if (compressionLevel <= 1) {
        zstdLevel=1;
    } else if (compressionLevel == 2) {
        zstdLevel=3;
    } else if (compressionLevel == 3) {
        zstdLevel=6;
    } else {
        zstdLevel=12;
    }

    openIOFileHandles(displayLevel, overwrite, input_filename, output_filename, &finput, &foutput);

    // Allocate Memory
    in_buff  = (char*)malloc(FOURMC_BLOCKSIZE);
    out_buff = (char*)malloc(FOURMC_BLOCKSIZE+CACHELINE);
    headerBuffer = (char*)malloc(FOURMC_HEADERSIZE);
    if (!in_buff || !out_buff || !(headerBuffer)) EXIT_WITH_FATALERROR("Allocation error : not enough memory");

    // Write Archive Header
    *(unsigned int*)headerBuffer = BIG_ENDIAN_32(FOURMZ_MAGICNUMBER);
    *(unsigned int*)(headerBuffer+4)  = BIG_ENDIAN_32(FOURMC_VERSION);

    checkbits = XXH32((headerBuffer), 8, 0);
    *(unsigned int*)(headerBuffer+8)  = BIG_ENDIAN_32(checkbits);
    header_size = FOURMC_HEADERSIZE;

    // Write header
    sizeCheck = fwrite(headerBuffer, 1, header_size, foutput);
    if (sizeCheck!=header_size) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write header");
    compressedfilesize += header_size;

    // read first block
    readSize = fread(in_buff, (size_t)1, (size_t)FOURMC_BLOCKSIZE, finput);

    // Main Loop
    while (readSize>0)
    {
        size_t outSize;

        // ------------------------------
        if (++blockIndexesCount >= blockIndexesReserved) {
            unsigned int curSize = blockIndexesReserved;
            unsigned long long* newBlockIndexes = (unsigned long long*)calloc(blockIndexesReserved*2,sizeof(unsigned long long));
            blockIndexesReserved*=2;
            for (bi=0; bi<curSize; ++bi) newBlockIndexes[bi] = blockIndexes[bi];
            free(blockIndexes);
            blockIndexes = newBlockIndexes;
        }
        blockIndexes[blockIndexesCount-1] = compressedfilesize;
        // ------------------------------

        filesize += readSize;
        CONSOLE_PRINT_LEVEL(3, "\rRead : %i MB   ", (int)(filesize>>20));


        // Compress Block
        outSize = ZSTD_compress(out_buff+12, (int)readSize-1, in_buff, (int)readSize, zstdLevel);

        if (!ZSTD_isError(outSize)) compressedfilesize += outSize+12; else compressedfilesize += readSize+12;
        CONSOLE_PRINT_LEVEL(3, "==> %.2f%%   ", (double)compressedfilesize/filesize*100);

        // Write Block
        if (!ZSTD_isError(outSize))
        {
            int sizeToWrite; unsigned int checksum;
            *(unsigned int*)(out_buff) = BIG_ENDIAN_32(readSize);
            *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(outSize);
            checksum = XXH32(out_buff+12, outSize, 0);
            *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(checksum);

            sizeToWrite = 12 + outSize;
            sizeCheck = fwrite(out_buff, 1, sizeToWrite, foutput);
            if (sizeCheck!=(size_t)(sizeToWrite)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write compressed block");
        }
        else  // Copy Original Uncompressed
        {
            unsigned int checksum;
            *(unsigned int*)(out_buff) = BIG_ENDIAN_32(readSize);
            *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(readSize);
            checksum = XXH32(in_buff, (int)readSize, 0);
            *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(checksum);
            sizeCheck = fwrite(out_buff, 1, 12, foutput);
            if (sizeCheck!=(size_t)(12)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write block header");
            sizeCheck = fwrite(in_buff, 1, readSize, foutput);
            if (sizeCheck!=readSize) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write block");
        }

        // Read next block
        readSize = fread(in_buff, (size_t)1, (size_t)FOURMC_BLOCKSIZE, finput);
    }

    // >>>> End of Stream mark <<<<
    *(unsigned int*)(out_buff) = BIG_ENDIAN_32(0);
    *(unsigned int*)(out_buff+4) = BIG_ENDIAN_32(0);
    *(unsigned int*)(out_buff+8) = BIG_ENDIAN_32(0);
    sizeCheck = fwrite(out_buff, 1, 12, foutput);
    if (sizeCheck!=(size_t)(12)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write end of stream");
    compressedfilesize += 12;

    // >>>> FOOTER <<<<
    free(headerBuffer);
    header_size = FOURMC_FOOTERSIZE(blockIndexesCount);
    headerBuffer = (char*)malloc(header_size);
    *(unsigned int*)(headerBuffer+0) = BIG_ENDIAN_32((unsigned int)header_size); // footer size
    *(unsigned int*)(headerBuffer+4) = BIG_ENDIAN_32(1); // version
    for (bi=0; bi<blockIndexesCount; ++bi) {
        unsigned int blockDelta;
        blockDelta = (unsigned int)(bi==0 ? ( blockIndexes[bi] ) : (blockIndexes[bi] - blockIndexes[bi-1]));
        *(unsigned long long*)(headerBuffer+8+(bi*4)) = BIG_ENDIAN_32(blockDelta);
        CONSOLE_PRINT_LEVEL(4, " * Block #%u at delta %u\n", bi, blockDelta);
    }
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4))   = BIG_ENDIAN_32((unsigned int)header_size); // footer size
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4)+4) = BIG_ENDIAN_32(FOURMZ_MAGICNUMBER); // footer size
    checkbits = XXH32(headerBuffer, header_size-4, 0);
    *(unsigned int*)(headerBuffer+8+(blockIndexesCount*4)+8) = BIG_ENDIAN_32(checkbits); // footer size

    sizeCheck = fwrite(headerBuffer, 1, header_size, foutput);
    if (sizeCheck!=(size_t)(header_size)) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write end of stream");
    compressedfilesize += header_size;

    // Close & Free
    free(in_buff);
    free(out_buff);
    free(headerBuffer);
    fclose(finput);
    fclose(foutput);
    free(blockIndexes);

    // Final Status
    end = clock();
    CONSOLE_PRINT_LEVEL(2, "\r%79s\r", "");
    CONSOLE_PRINT_LEVEL(2, "Compressed (%s) %llu bytes into %llu bytes ==> %.2f%% (Ratio=%.3f)\n",
                        compressionLevel<=1?"fast":(compressionLevel==2?"medium":(compressionLevel==3?"high":"ultra")),
                        (unsigned long long) filesize, (unsigned long long) compressedfilesize, (double)compressedfilesize/filesize*100,
                        (double)100.0/((double)compressedfilesize/filesize*100));

    {
        double seconds = (double)(end - start)/CLOCKS_PER_SEC;
        CONSOLE_PRINT_LEVEL(4, "Done in %.2f s ==> %.2f MB/s\n", seconds, (double)filesize / seconds / 1024 / 1024);
    }

    return 0;
}

/* ********************************************************************* */
/* ********************** LZ4 File / Stream decoding ******************* */
/* ********************************************************************* */


static unsigned long long decodeFourMC(int displayLevel, FILE* finput, FILE* foutput)
{
    unsigned long long filesize = 0;
    char* in_buff;
    size_t in_buff_size=0, out_buff_size=0;
    char* out_buff;
    char * descriptor;
    size_t nbReadBytes;
    int decodedBytes=0;
    size_t sizeCheck;
    unsigned int footerSize, checksum;

    descriptor = (char*)malloc(FOURMC_HEADERSIZE*2); // will be used for file header(12), block header(12), others(<12)

    // Decode stream descriptor
    *(unsigned int*)(descriptor) = BIG_ENDIAN_32(FOURMC_MAGICNUMBER); // magic

    nbReadBytes = fread(descriptor+4, 1, FOURMC_HEADERSIZE-4, finput);
    if (nbReadBytes != (FOURMC_HEADERSIZE-4)) EXIT_WITH_FATALERROR_CONTENT("Unreadable header");
    {
        unsigned int version       = BIG_ENDIAN_32(*(unsigned int*)(descriptor+4));
        unsigned int checkSum      = BIG_ENDIAN_32(*(unsigned int*)(descriptor+8));
        unsigned int calcCS        = XXH32(descriptor, 8, 0);
        if (version != 1)       EXIT_WITH_FATALERROR_CONTENT("Wrong version number");
        if (checkSum!=calcCS)   EXIT_WITH_FATALERROR_CONTENT("Wrong header checksum");
    }

    // Allocate Memory
    {
        in_buff_size = FOURMC_BLOCKSIZE;
        out_buff_size = FOURMC_BLOCKSIZE;
        in_buff  = (char*)malloc(in_buff_size);
        out_buff = (char*)malloc(out_buff_size);
        if (!in_buff || !out_buff) EXIT_WITH_FATALERROR("Allocation error : not enough memory");
    }

    /**
        Uncompressed size:    4 bytes
        Compressed size:    4 bytes, if compressed size==uncompressed size, then the data is stored as plain
        Checksum:            4 bytes, calculated on the compressed data
     */

    // Main Loop
    while (1)
    {
        char* blockHeader = descriptor;
        unsigned int uncompressedSize, compressedSize, blockCheckSum;

        // Block Size
        nbReadBytes = fread(blockHeader, 1, 12, finput);
        if( nbReadBytes != 12 ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read next block size");

        uncompressedSize = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+0));
        compressedSize = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+4));
        blockCheckSum = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+8));

        if (uncompressedSize==0 && compressedSize==0 && blockCheckSum==0) break;

        if (compressedSize>FOURMC_BLOCKSIZE) {
            EXIT_WITH_FATALERROR_CONTENT("Read error: block size beyond 4MB limit");
        }

        if ((in_buff_size < compressedSize)) {
            free(in_buff);
            while (in_buff_size < compressedSize) {
                in_buff_size*=2;
            }
            in_buff  = (char*)malloc(in_buff_size);
        }

        // Read Block
        nbReadBytes = fread(in_buff, 1, compressedSize, finput);
        if( nbReadBytes != compressedSize ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read data block" );

        // Check Block
        if (uncompressedSize==compressedSize) // uncompressed!
        {
            checksum = XXH32(in_buff, compressedSize, 0);
            if (checksum != blockCheckSum) EXIT_WITH_FATALERROR_CONTENT("Error : invalid block checksum detected");
            sizeCheck = fwrite(in_buff, 1, uncompressedSize, foutput);
            if (sizeCheck != (size_t)uncompressedSize) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write data block");
            filesize += uncompressedSize;
        }
        else // compressed block
        {
            checksum = XXH32(in_buff, compressedSize, 0);
            if (checksum != blockCheckSum) EXIT_WITH_FATALERROR_CONTENT("Error : invalid block checksum detected");

            if ((out_buff_size < uncompressedSize)) {
                free(out_buff);

                if (uncompressedSize>FOURMC_BLOCKSIZE) {
                    EXIT_WITH_FATALERROR_CONTENT("Read error: uncompressed block size beyond 4MB limit");
                }

                while (out_buff_size < uncompressedSize) {
                    out_buff_size*=2;
                }
                out_buff  = (char*)malloc(out_buff_size);
            }

            decodedBytes = LZ4_decompress_safe (in_buff, out_buff, compressedSize, uncompressedSize);
            if (decodedBytes < 0) EXIT_WITH_FATALERROR_CONTENT("Decoding Failed ! Corrupted input detected !");
            filesize += decodedBytes;

            sizeCheck = fwrite(out_buff, 1, decodedBytes, foutput);
            if (sizeCheck != (size_t)decodedBytes) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write decoded block\n");
        }
    }

    // read and decode footer
    nbReadBytes = fread(descriptor, 1, 4, finput);
    if (nbReadBytes != 4) EXIT_WITH_FATALERROR("Unreadable footer");
    footerSize = BIG_ENDIAN_32(*(unsigned int*)(descriptor));
    if ((in_buff_size < footerSize)) {
        free(in_buff);
        in_buff_size = footerSize;
        in_buff  = (char*)malloc(in_buff_size);
    }
    nbReadBytes = fread(in_buff+4, 1, footerSize-4, finput);
    if (nbReadBytes != (footerSize-4) ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read footer" );
    *(unsigned int*)(in_buff) = *(unsigned int*)(descriptor);

    // checksum
    checksum = XXH32(in_buff, footerSize-4, 0);
    if (checksum != BIG_ENDIAN_32(*(unsigned int*)(in_buff+footerSize-4))) EXIT_WITH_FATALERROR_CONTENT("Error : invalid footer checksum detected");

    if ( BIG_ENDIAN_32(*(unsigned int*)(in_buff+4)) != 1) // check footer version
    EXIT_WITH_FATALERROR_CONTENT("Read error : unsupported footer version" );

    if (displayLevel>=3) {
        unsigned long long absOffset=0;
        unsigned int i, totalBlockIndexes = (footerSize-20)/4;
        CONSOLE_PRINT_LEVEL(3, "\nBlock index %u entries:\n", totalBlockIndexes);
        for (i=0; i<totalBlockIndexes; ++i) {
            unsigned int delta = BIG_ENDIAN_32(*(unsigned int*)(in_buff+8+i*4));
            absOffset += delta;
            CONSOLE_PRINT_LEVEL(3, " * Block #%u at %llu (+%u)\n", i, absOffset, delta);
        }
    }

    // Free
    free(descriptor);
    free(in_buff);
    free(out_buff);

    return filesize;
}

static unsigned long long decodeFourMZ(int displayLevel, FILE* finput, FILE* foutput)
{
    unsigned long long filesize = 0;
    char* in_buff;
    size_t in_buff_size=0, out_buff_size=0;
    char* out_buff;
    char * descriptor;
    size_t nbReadBytes;
    int decodedBytes=0;
    size_t sizeCheck;
    unsigned int footerSize, checksum;

    descriptor = (char*)malloc(FOURMC_HEADERSIZE*2); // will be used for file header(12), block header(12), others(<12)

    // Decode stream descriptor
    *(unsigned int*)(descriptor) = BIG_ENDIAN_32(FOURMZ_MAGICNUMBER); // magic

    nbReadBytes = fread(descriptor+4, 1, FOURMC_HEADERSIZE-4, finput);
    if (nbReadBytes != (FOURMC_HEADERSIZE-4)) EXIT_WITH_FATALERROR_CONTENT("Unreadable header");
    {
        unsigned int version       = BIG_ENDIAN_32(*(unsigned int*)(descriptor+4));
        unsigned int checkSum      = BIG_ENDIAN_32(*(unsigned int*)(descriptor+8));
        unsigned int calcCS        = XXH32(descriptor, 8, 0);
        if (version != 1)       EXIT_WITH_FATALERROR_CONTENT("Wrong version number");
        if (checkSum!=calcCS)   EXIT_WITH_FATALERROR_CONTENT("Wrong header checksum");
    }

    // Allocate Memory
    {
        in_buff_size = FOURMC_BLOCKSIZE;
        out_buff_size = FOURMC_BLOCKSIZE;
        in_buff  = (char*)malloc(in_buff_size);
        out_buff = (char*)malloc(out_buff_size);
        if (!in_buff || !out_buff) EXIT_WITH_FATALERROR("Allocation error : not enough memory");
    }

    /**
        Uncompressed size:    4 bytes
        Compressed size:    4 bytes, if compressed size==uncompressed size, then the data is stored as plain
        Checksum:            4 bytes, calculated on the compressed data
     */

    // Main Loop
    while (1)
    {
        char* blockHeader = descriptor;
        unsigned int uncompressedSize, compressedSize, blockCheckSum;

        // Block Size
        nbReadBytes = fread(blockHeader, 1, 12, finput);
        if( nbReadBytes != 12 ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read next block size");

        uncompressedSize = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+0));
        compressedSize = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+4));
        blockCheckSum = BIG_ENDIAN_32(*(unsigned int*)(blockHeader+8));

        if (uncompressedSize==0 && compressedSize==0 && blockCheckSum==0) break;

        if (compressedSize>FOURMC_BLOCKSIZE) {
            EXIT_WITH_FATALERROR_CONTENT("Read error: block size beyond 4MB limit");
        }

        if ((in_buff_size < compressedSize)) {
            free(in_buff);
            while (in_buff_size < compressedSize) {
                in_buff_size*=2;
            }
            in_buff  = (char*)malloc(in_buff_size);
        }

        // Read Block
        nbReadBytes = fread(in_buff, 1, compressedSize, finput);
        if( nbReadBytes != compressedSize ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read data block" );

        // Check Block
        if (uncompressedSize==compressedSize) // uncompressed!
        {
            checksum = XXH32(in_buff, compressedSize, 0);
            if (checksum != blockCheckSum) EXIT_WITH_FATALERROR_CONTENT("Error : invalid block checksum detected");
            sizeCheck = fwrite(in_buff, 1, uncompressedSize, foutput);
            if (sizeCheck != (size_t)uncompressedSize) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write data block");
            filesize += uncompressedSize;
        }
        else // compressed block
        {
            checksum = XXH32(in_buff, compressedSize, 0);
            if (checksum != blockCheckSum) EXIT_WITH_FATALERROR_CONTENT("Error : invalid block checksum detected");

            if ((out_buff_size < uncompressedSize)) {
                free(out_buff);

                if (uncompressedSize>FOURMC_BLOCKSIZE) {
                    EXIT_WITH_FATALERROR_CONTENT("Read error: uncompressed block size beyond 4MB limit");
                }

                while (out_buff_size < uncompressedSize) {
                    out_buff_size*=2;
                }
                out_buff  = (char*)malloc(out_buff_size);
            }

            decodedBytes = ZSTD_decompress(out_buff, uncompressedSize, in_buff, compressedSize);

            if (decodedBytes < 0) EXIT_WITH_FATALERROR_CONTENT("Decoding Failed ! Corrupted input detected !");
            filesize += decodedBytes;

            sizeCheck = fwrite(out_buff, 1, decodedBytes, foutput);
            if (sizeCheck != (size_t)decodedBytes) EXIT_WITH_FATALERROR_OUTPUT("Write error : cannot write decoded block\n");
        }
    }

    // read and decode footer
    nbReadBytes = fread(descriptor, 1, 4, finput);
    if (nbReadBytes != 4) EXIT_WITH_FATALERROR("Unreadable footer");
    footerSize = BIG_ENDIAN_32(*(unsigned int*)(descriptor));
    if ((in_buff_size < footerSize)) {
        free(in_buff);
        in_buff_size = footerSize;
        in_buff  = (char*)malloc(in_buff_size);
    }
    nbReadBytes = fread(in_buff+4, 1, footerSize-4, finput);
    if (nbReadBytes != (footerSize-4) ) EXIT_WITH_FATALERROR_INPUT("Read error : cannot read footer" );
    *(unsigned int*)(in_buff) = *(unsigned int*)(descriptor);

    // checksum
    checksum = XXH32(in_buff, footerSize-4, 0);
    if (checksum != BIG_ENDIAN_32(*(unsigned int*)(in_buff+footerSize-4))) EXIT_WITH_FATALERROR_CONTENT("Error : invalid footer checksum detected");

    if ( BIG_ENDIAN_32(*(unsigned int*)(in_buff+4)) != 1) // check footer version
    EXIT_WITH_FATALERROR_CONTENT("Read error : unsupported footer version" );

    if (displayLevel>=3) {
        unsigned long long absOffset=0;
        unsigned int i, totalBlockIndexes = (footerSize-20)/4;
        CONSOLE_PRINT_LEVEL(3, "\nBlock index %u entries:\n", totalBlockIndexes);
        for (i=0; i<totalBlockIndexes; ++i) {
            unsigned int delta = BIG_ENDIAN_32(*(unsigned int*)(in_buff+8+i*4));
            absOffset += delta;
            CONSOLE_PRINT_LEVEL(3, " * Block #%u at %llu (+%u)\n", i, absOffset, delta);
        }
    }

    // Free
    free(descriptor);
    free(in_buff);
    free(out_buff);

    return filesize;
}

/**
 * Generic implementation like LZ4 stream, but in the end we just have 1 format atm.
 */
static unsigned long long selectDecoder(int displayLevel, FILE* finput,  FILE* foutput)
{
    unsigned int magicNumber;
    size_t nbReadBytes;

    // Check Archive Header
    nbReadBytes = fread(&magicNumber, 1, MAGICNUMBER_SIZE, finput);
    if (nbReadBytes==0) return 0;                  // EOF
    if (nbReadBytes != MAGICNUMBER_SIZE) EXIT_WITH_FATALERROR_CONTENT("Unrecognized header : Magic Number unreadable");
    magicNumber = BIG_ENDIAN_32(magicNumber);

    if (magicNumber != FOURMC_MAGICNUMBER) EXIT_WITH_FATALERROR_CONTENT("Unrecognized header : not a 4mc file");

    return decodeFourMC(displayLevel, finput, foutput);
}

static unsigned long long selectZstdDecoder(int displayLevel, FILE* finput,  FILE* foutput)
{
    unsigned int magicNumber;
    size_t nbReadBytes;

    // Check Archive Header
    nbReadBytes = fread(&magicNumber, 1, MAGICNUMBER_SIZE, finput);
    if (nbReadBytes==0) return 0;                  // EOF
    if (nbReadBytes != MAGICNUMBER_SIZE) EXIT_WITH_FATALERROR_CONTENT("Unrecognized header : Magic Number unreadable");
    magicNumber = BIG_ENDIAN_32(magicNumber);

    if (magicNumber != FOURMZ_MAGICNUMBER) EXIT_WITH_FATALERROR_CONTENT("Unrecognized header : not a 4mc file");

    return decodeFourMZ(displayLevel, finput, foutput);
}



int fourMcDecompressFileName(int displayLevel, int overwrite, char* input_filename, char* output_filename)
{
    unsigned long long filesize = 0, decodedSize=0;
    FILE* finput;
    FILE* foutput;
    clock_t start, end;

    // Init
    start = clock();
    openIOFileHandles(displayLevel, overwrite, input_filename, output_filename, &finput, &foutput);

    // Loop over multiple streams
    do
    {
        decodedSize = selectDecoder(displayLevel, finput, foutput);
        filesize += decodedSize;
    } while (decodedSize);

    // Final Status
    end = clock();
    CONSOLE_PRINT_LEVEL(2, "\r%79s\r", "");
    CONSOLE_PRINT_LEVEL(2, "Successfully decoded %llu bytes \n", filesize);
    {
        double seconds = (double)(end - start)/CLOCKS_PER_SEC;
        CONSOLE_PRINT_LEVEL(4, "Done in %.2f s ==> %.2f MB/s\n", seconds, (double)filesize / seconds / 1024 / 1024);
    }

    // Close
    fclose(finput);
    fclose(foutput);

    // Error status = OK
    return 0;
}

int fourMZDecompressFileName(int displayLevel, int overwrite, char* input_filename, char* output_filename)
{
    unsigned long long filesize = 0, decodedSize=0;
    FILE* finput;
    FILE* foutput;
    clock_t start, end;

    // Init
    start = clock();
    openIOFileHandles(displayLevel, overwrite, input_filename, output_filename, &finput, &foutput);

    // Loop over multiple streams
    do
    {
        decodedSize = selectZstdDecoder(displayLevel, finput, foutput);
        filesize += decodedSize;
    } while (decodedSize);

    // Final Status
    end = clock();
    CONSOLE_PRINT_LEVEL(2, "\r%79s\r", "");
    CONSOLE_PRINT_LEVEL(2, "Successfully decoded %llu bytes \n", filesize);
    {
        double seconds = (double)(end - start)/CLOCKS_PER_SEC;
        CONSOLE_PRINT_LEVEL(4, "Done in %.2f s ==> %.2f MB/s\n", seconds, (double)filesize / seconds / 1024 / 1024);
    }

    // Close
    fclose(finput);
    fclose(foutput);

    // Error status = OK
    return 0;
}