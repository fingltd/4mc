PROJECT(4MC C)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "4mc Four More Compression")
set(CPACK_PACKAGE_VERSION_MAJOR 2)
set(CPACK_PACKAGE_VERSION_MINOR 0)
set(CPACK_PACKAGE_VERSION_PATCH 0)
set(VERSION_STRING	" \"${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}\" ")
include(CPack)
include(FindJNI)

cmake_minimum_required (VERSION 2.6)
INCLUDE (CheckTypeSize)
check_type_size("void *" SIZEOF_VOID_P)
IF( ${SIZEOF_VOID_P} STREQUAL  "8" )
	set (CMAKE_SYSTEM_PROCESSOR "64bit")
	MESSAGE( STATUS "64 bit architecture detected size of void * is " ${SIZEOF_VOID_P})
ENDIF()

if(APPLE)
	execute_process(COMMAND sysctl -q hw.optional.arm64
			OUTPUT_VARIABLE _sysctl_stdout
			ERROR_VARIABLE _sysctl_stderr
			RESULT_VARIABLE _sysctl_result
			)

	if(_sysctl_result EQUAL 0 AND _sysctl_stdout MATCHES "hw.optional.arm64: 1" AND APPLE_x86_64)
		SET(CMAKE_SYSTEM_PROCESSOR "x86_64")
		SET(CMAKE_OSX_ARCHITECTURES "x86_64")
		MESSAGE(STATUS "x86_64 bit architecture has been configured - arm64 detected")
	endif()
ENDIF(APPLE)

option(BUILD_TOOLS "Build the command line tools" ON)
option(BUILD_LIBS  "Build the JNI native library for hadoop-4mc" ON)

if(UNIX AND NOT APPLE AND BUILD_LIBS)
	if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
		add_definitions(-fPIC)
	endif()
endif()

IF (MSVC)
	# MSVC is logs many inline warnings
	add_definitions("/wd4710")
	add_definitions("/wd4711")
ENDIF()

if (MSVC OR APPLE)
	add_compile_options(-DZSTD_DISABLE_ASM)
endif(MSVC OR APPLE)

set(LZ4_DIR ../lz4/)
set(ZSTD_DIR ../zstd/)
set(PRG_DIR ../)
set(LZ4_SRCS ${LZ4_DIR}xxhash.h ${LZ4_DIR}xxhash.c ${LZ4_DIR}lz4.c ${LZ4_DIR}lz4hc.c ${LZ4_DIR}lz4.h ${LZ4_DIR}lz4hc.h ${LZ4_DIR}lz4mc.c ${LZ4_DIR}lz4mc.h ${LZ4_DIR}lz4frame.h ${LZ4_DIR}lz4frame.c ${LZ4_DIR}lz4file.h ${LZ4_DIR}lz4file.c)
set(ZSTD_SRCS
		${ZSTD_DIR}common/entropy_common.c
		${ZSTD_DIR}common/error_private.c
		${ZSTD_DIR}common/fse_decompress.c
		${ZSTD_DIR}common/pool.c
		${ZSTD_DIR}common/zstd_common.c
		${ZSTD_DIR}common/xxhash.c
		${ZSTD_DIR}compress/fse_compress.c
		${ZSTD_DIR}compress/hist.c
		${ZSTD_DIR}compress/huf_compress.c
		${ZSTD_DIR}compress/zstd_compress.c
		${ZSTD_DIR}compress/zstd_compress_literals.c
		${ZSTD_DIR}compress/zstd_compress_sequences.c
		${ZSTD_DIR}compress/zstd_compress_superblock.c
		${ZSTD_DIR}compress/zstd_double_fast.c
		${ZSTD_DIR}compress/zstd_fast.c
		${ZSTD_DIR}compress/zstd_lazy.c
		${ZSTD_DIR}compress/zstd_ldm.c
		${ZSTD_DIR}compress/zstd_opt.c
		${ZSTD_DIR}compress/zstdmt_compress.c
		${ZSTD_DIR}decompress/huf_decompress.c
		${ZSTD_DIR}decompress/zstd_ddict.c
		${ZSTD_DIR}decompress/zstd_decompress_block.c
		${ZSTD_DIR}decompress/zstd_decompress.c
		${ZSTD_DIR}decompress/huf_decompress_amd64.S
		${ZSTD_DIR}deprecated/zbuff_compress.c
		${ZSTD_DIR}deprecated/zbuff_decompress.c
		${ZSTD_DIR}dictBuilder/cover.c
		${ZSTD_DIR}dictBuilder/divsufsort.c
		${ZSTD_DIR}dictBuilder/fastcover.c
		${ZSTD_DIR}dictBuilder/zdict.c
		)
set(4MC_SRCS ${PRG_DIR}4mccli.c ${PRG_DIR}4mc.c)
set(HADOOP4MC_SRCS ${PRG_DIR}jniCompressor.c ${PRG_DIR}jniDecompressor.c ${PRG_DIR}jniZstdCompressor.c ${PRG_DIR}jniZstdDecompressor.c
		${PRG_DIR}jniZstd.c ${PRG_DIR}jniZStreamCompressor.c ${PRG_DIR}jniZStreamDecompressor.c)

if(NOT WIN32)
	add_definitions(-O3)
	add_definitions("-std=c99")
endif()

INCLUDE_DIRECTORIES(../zstd ../zstd/common)

if(BUILD_TOOLS)
	add_executable(4mc ${LZ4_SRCS} ${ZSTD_SRCS} ${4MC_SRCS})
	install(TARGETS 4mc RUNTIME DESTINATION "bin/")
endif()

if(BUILD_LIBS)
	find_package(JNI)
	if (JNI_FOUND)
		message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
		message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
		INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})
	else()
		MESSAGE(FATAL_ERROR "Found JNI: failed")
	endif()

	add_library(hadoop-4mc SHARED ${LZ4_SRCS} ${ZSTD_SRCS} ${HADOOP4MC_SRCS})
	install(TARGETS 4mc RUNTIME DESTINATION "lib/")
endif()

#warnings

ADD_DEFINITIONS("-Wall")
ADD_DEFINITIONS("-DLZ4_VERSION=\"1.9.4\"")
INCLUDE_DIRECTORIES (${LZ4_DIR})



