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



//**************************************
// Compiler Options (from lz4cli.c)
//**************************************
// Disable some Visual warning messages
#ifdef _MSC_VER  // Visual Studio
#  define _CRT_SECURE_NO_WARNINGS
#  define _CRT_SECURE_NO_DEPRECATE     // VS2005
#  pragma warning(disable : 4127)      // disable: C4127: conditional expression is constant
#endif

#define _FILE_OFFSET_BITS 64   // Large file support on 32-bits unix
#define _POSIX_SOURCE 1        // for fileno() within <stdio.h> on unix


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "lz4/lz4.h"
#include "lz4/lz4hc.h"
#include "lz4/xxhash.h"

#include "4mc.h"


#if defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(_WIN32) || defined(__CYGWIN__)
#  include <fcntl.h>    // _O_BINARY
#  include <io.h>       // _setmode, _isatty
#  ifdef __MINGW32__
   int _fileno(FILE *stream);   // MINGW somehow forgets to include this windows declaration into <stdio.h>
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


#define FOURMC_VERSION "v2.0.0"
#define AUTHOR "Carlo Medas"
#define WELCOME_MESSAGE "*** 4mc CLI %i-bits %s by %s (%s) ***\n*** Unleashing the power of LZ4 and ZSTD by Yann Collet/Facebook ***\n", (int)(sizeof(void*)*8), FOURMC_VERSION, AUTHOR, __DATE__
#define FOURMC_EXTENSION ".4mc"
#define FOURMZ_EXTENSION ".4mz"

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)



static const int one = 1;
#define CPU_LITTLE_ENDIAN   (*(char*)(&one))
#define CPU_BIG_ENDIAN      (!CPU_LITTLE_ENDIAN)
#define LITTLE_ENDIAN_32(i) (CPU_LITTLE_ENDIAN?(i):swap32(i))


#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }


static char* programName;


#define DEBUG 0
#define DEBUGOUTPUT(...) if (DEBUG) DISPLAY(__VA_ARGS__);
#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DEBUGOUTPUT("Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAYLEVEL(1, "Error %i : ", error);                                \
    DISPLAYLEVEL(1, __VA_ARGS__);                                         \
    DISPLAYLEVEL(1, "\n");                                                \
    exit(error);                                                          \
}



int usage(void)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [arg] [input] [output]\n", programName);
    DISPLAY( "\n");
    DISPLAY( "input   : a filename\n");
    DISPLAY( "          with no FILE, or when FILE is - or %s, read standard input\n", stdinmark);
    DISPLAY( "Arguments :\n");
    DISPLAY( " -z     : zstd compression (default is LZ4) \n");
    DISPLAY( " -1     : Fast compression (default) \n");
    DISPLAY( " -2     : Medium compression \n");
    DISPLAY( " -3     : High compression \n");
    DISPLAY( " -4     : Ultra compression \n");
    DISPLAY( " -d     : decompression (default for %s and %s exts)\n", FOURMC_EXTENSION, FOURMZ_EXTENSION);
    DISPLAY( " -f     : overwrite output without prompting \n");
    DISPLAY( " -V     : display Version number and exit\n");
    DISPLAY( " -v     : verbose mode\n");
    DISPLAY( " -q     : quiet mode\n");
    DISPLAY( " -h     : display help and exit\n");
    return 0;
}


int badusage(int displayLevel)
{
    DISPLAYLEVEL(1, "Incorrect command line arguments\n");
    if (displayLevel >= 1) usage();
    exit(1);
}


void waitEnter(void)
{
    DISPLAY("Press enter to continue...\n");
    getchar();
}


int main(int argc, char** argv)
{
    int i, cLevel=0,  decode=0,  filenamesStart=2, legacy_format=0,
        forceStdout=0, forceCompress=0, promptPause=0, overwriteMode=0, zstdMode=0;

    // Display level modes exactly like LZ4 CLI
    // 0 : no display  // 1: errors  // 2 : + result + interaction + warnings ;  // 3 : + progression;  // 4 : + information
    int displayLevel = 2;

    char* input_filename=0;
    char* output_filename=0;
    char* dynNameSpace=0;
    char nullOutput[] = NULL_OUTPUT;
    char extension4mc[] = FOURMC_EXTENSION;
    char extension4mz[] = FOURMZ_EXTENSION;

    // Init
    programName = argv[0];

    // command switches
    for(i=1; i<argc; i++)
    {
        char* argument = argv[i];

        if(!argument) continue;   // Protection if argument empty

        // Decode command (note : aggregated commands are allowed)
        if (argument[0]=='-')
        {
            // '-' means stdin/stdout
            if (argument[1]==0)
            {
                if (!input_filename) input_filename=stdinmark;
                else output_filename=stdoutmark;
            }

            while (argument[1]!=0)
            {
                argument++;

                if ((*argument>='0') && (*argument<='9'))
                {
                    cLevel = 0;
                    while ((*argument >= '0') && (*argument <= '9'))
                    {
                        cLevel *= 10;
                        cLevel += *argument - '0';
                        argument++;
                    }
                    argument--;
                    continue;
                }

                switch(argument[0])
                {
                    // Display help
                case 'V': DISPLAY(WELCOME_MESSAGE); return 0;   // Version
                case 'h': usage(); return 0;
                case 'H': usage(); return 0;

                    // ZSTD (default is LZ4)
                case 'z': zstdMode = 1; forceCompress=1; break;

                    // Use Legacy format (for Linux kernel compression)
                case 'l': legacy_format=1; break;

                    // Decoding
                case 'd': decode=1; break;

                    // Force stdout, even if stdout==console
                case 'c': forceStdout=1; output_filename=stdoutmark; displayLevel=1; break;

                    // Test
                case 't': decode=1; output_filename=nulmark; break;

                    // Overwrite
                case 'f': overwriteMode=1; break;

                    // Verbose mode
                case 'v': displayLevel=4; break;

                    // Quiet mode
                case 'q': displayLevel--; break;

                    // Unrecognised command
                default : badusage(displayLevel);
                }
            }
            continue;
        }

        // first provided filename is input
        if (!input_filename) { input_filename=argument; filenamesStart=i; continue; }

        // second provided filename is output
        if (!output_filename)
        {
            output_filename=argument;
            if (!strcmp (output_filename, nullOutput)) output_filename = nulmark;
            continue;
        }
    }

    DISPLAYLEVEL(3, WELCOME_MESSAGE);

    // No input filename ==> use stdin
    if(!input_filename) { input_filename=stdinmark; }

    // Check if input or output are defined as console; trigger an error in this case
    if (!strcmp(input_filename, stdinmark)  && IS_CONSOLE(stdin)) badusage(displayLevel);


    // No output filename ==> try to select one automatically like lz4
    while (!output_filename)
    {
        if (!IS_CONSOLE(stdout)) { output_filename=stdoutmark; break; }   // Default to stdout whenever possible (i.e. not a console)
        if ((!decode) && !(forceCompress))   // auto-determine compression or decompression, based on file extension
        {
            size_t l = strlen(input_filename);
            if (!strcmp(input_filename+(l-4), FOURMC_EXTENSION) ||
            	!strcmp(input_filename+(l-4), FOURMZ_EXTENSION)) decode=1;
        }
        if (!decode)   // compression to file
        {
            size_t l = strlen(input_filename);
            dynNameSpace = (char*)calloc(1,l+5);
            output_filename = dynNameSpace;
            strcpy(output_filename, input_filename);
            if (zstdMode) {
            	strcpy(output_filename+l, FOURMZ_EXTENSION);
            } else {
            	strcpy(output_filename+l, FOURMC_EXTENSION);
        	}
            DISPLAYLEVEL(2, "Compressed filename will be : %s \n", output_filename);
            break;
        }
        // decompression to file (automatic name will work only if input filename has correct format extension)
        {
            size_t outl;
            size_t inl = strlen(input_filename);
            dynNameSpace = (char*)calloc(1,inl+1);
            output_filename = dynNameSpace;
            strcpy(output_filename, input_filename);
            outl = inl;
            if (inl>4)
                while ((outl >= inl-4) && (input_filename[outl] ==  extension4mc[outl-inl+4]))
                	output_filename[outl--]=0;

            if (outl != inl-5 && inl>4) {
            	outl = inl;
            	while ((outl >= inl-4) && (input_filename[outl] ==  extension4mz[outl-inl+4]))
            	                	output_filename[outl--]=0;
            	zstdMode=1;
            }

            if (outl != inl-5) { DISPLAYLEVEL(1, "Cannot determine an output filename\n"); badusage(displayLevel); }
            DISPLAYLEVEL(2, "Decoding file %s \n", output_filename);
            if (zstdMode) {
            	DISPLAYLEVEL(2, "Compression: ZSTD\n");
            } else {
            	DISPLAYLEVEL(2, "Compression: LZ4\n");
            }
        }
    }

    // No warning message in pure pipe mode (stdin + stdout)
    if (!strcmp(input_filename, stdinmark) && !strcmp(output_filename,stdoutmark) && (displayLevel==2)) displayLevel=1;

    // Check if input or output are defined as console; trigger an error in this case
    if (!strcmp(input_filename, stdinmark)  && IS_CONSOLE(stdin)                 ) badusage(displayLevel);
    if (!strcmp(output_filename,stdoutmark) && IS_CONSOLE(stdout) && !forceStdout) badusage(displayLevel);

    if (decode) {
    	if (zstdMode==0) {
    		fourMcDecompressFileName(displayLevel, overwriteMode, input_filename, output_filename);
    	} else {
    		fourMZDecompressFileName(displayLevel, overwriteMode, input_filename, output_filename);
    	}
    } else {
    	if (zstdMode==0) {
    		DISPLAYLEVEL(2, "Compression: LZ4\n");
    		fourMCcompressFilename(displayLevel, overwriteMode, input_filename, output_filename, cLevel);
    	} else {
    		DISPLAYLEVEL(2, "Compression: ZSTD\n");
    		fourMZcompressFilename(displayLevel, overwriteMode, input_filename, output_filename, cLevel);
    	}
    }

    if (promptPause) waitEnter();
    free(dynNameSpace);
    return 0;
}
