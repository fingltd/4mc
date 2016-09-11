# ##########################################################################
# 4MC program - Makefile
# Copyright (C) Carlo Medas 2014 
# Used LZ4 code - Copyright (C) Yann Collet 2011-2014
# ##########################################################################
# 4mc : Command Line Utility, supporting gzip-like arguments
# ##########################################################################

RELEASE=r121
DESTDIR=
PREFIX=/usr
CC:=$(CC)
CFLAGS?= -O3
#CFLAGS?= -ggdb
CFLAGS+= -std=c99 -Wall -Wextra -Wundef -Wshadow -Wstrict-prototypes -DLZ4_VERSION=\"$(RELEASE)\"
FLAGS= -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include/darwin $(CPPFLAGS) $(CFLAGS) $(LDFLAGS)
CPPFLAGS= -I./zstd -I./zstd/common

BINDIR=$(PREFIX)/bin
MANDIR=$(PREFIX)/share/man/man1
LZ4DIR=..

LIBVER_MAJOR=1
LIBVER_MINOR=1
LIBVER_PATCH=0
LIBVER=$(LIBVER_MAJOR).$(LIBVER_MINOR).$(LIBVER_PATCH)

ZSTD_FILES := zstd/common/*.c zstd/compress/*.c zstd/decompress/*.c zstd/dictBuilder/*.c

ifeq ($(shell uname), Darwin)
	SHARED_EXT = dylib
	SHARED_EXT_MAJOR = $(LIBVER_MAJOR).$(SHARED_EXT)
	SHARED_EXT_VER = $(LIBVER).$(SHARED_EXT)
	SONAME_FLAGS = -install_name $(PREFIX)/lib/libhadoop-4mc.$(SHARED_EXT_MAJOR) -compatibility_version $(LIBVER_MAJOR) -current_version $(LIBVER)
else
	SONAME_FLAGS = -Wl,-soname=libhadoop-4mc.$(SHARED_EXT).$(LIBVER_MAJOR)
	SHARED_EXT = so
	SHARED_EXT_MAJOR = $(SHARED_EXT).$(LIBVER_MAJOR)
	SHARED_EXT_VER = $(SHARED_EXT).$(LIBVER)
endif

# Define *.exe as extension for Windows systems
ifneq (,$(filter Windows%,$(OS)))
EXT =.exe
VOID = nul
else
EXT =
VOID = /dev/null
endif


default: 4mc 

all: 4mc libhadoop-4mc

4mc: lz4/lz4.c lz4/lz4hc.c lz4/lz4mc.c lz4/xxhash.c $(ZSTD_FILES) 4mc.c 4mccli.c 
	$(CC)      $(FLAGS) $^ -o $@$(EXT)

libhadoop-4mc: lz4/lz4.c lz4/lz4hc.c lz4/lz4mc.c lz4/xxhash.c $(ZSTD_FILES) jniDecompressor.c jniCompressor.c jniZstdDecompressor.c jniZstdCompressor.c 
	@$(CC) $(FLAGS) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) -shared $^ -fPIC $(SONAME_FLAGS) -o $@.$(SHARED_EXT_VER)


clean:
	@rm -f core *.o \
        4mc$(EXT) \
	libhadoop-4mc.$(SHARED_EXT_VER)
	@echo Cleaning completed


#------------------------------------------------------------------------
#make install is validated only for Linux, OSX, kFreeBSD and Hurd targets
ifneq (,$(filter $(shell uname),Linux Darwin GNU/kFreeBSD GNU))

install: 4mc 
	@echo Installing binaries
	@install -d -m 755 $(DESTDIR)$(BINDIR)/ $(DESTDIR)$(MANDIR)/
	@install -m 755 4mc   $(DESTDIR)$(BINDIR)/4mc
	@echo Installing man pages
	@install -m 644 4mc.1 $(DESTDIR)$(MANDIR)/4mc.1
	@echo 4mc installation completed

uninstall:
	[ -x $(DESTDIR)$(BINDIR)/4mc ] && rm -f $(DESTDIR)$(BINDIR)/4mc
	@echo 4mc successfully uninstalled


endif
