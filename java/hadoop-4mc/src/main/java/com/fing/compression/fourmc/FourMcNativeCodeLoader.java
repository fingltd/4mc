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
package com.fing.compression.fourmc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class FourMcNativeCodeLoader {
    private static final Log LOG = LogFactory.getLog(FourMcNativeCodeLoader.class);
    private static boolean nativeLibraryLoaded = false;

    /**
     * The system property to force 4mc library to load from the library path,
     * thus ignoring the embedded libraries inside jar.
     */
    public static final String USE_BINARIES_ON_LIB_PATH =
            "com.fing.compression.fourmc.use.libpath";

    private enum OS {
        WINDOWS("win32", "dll"), LINUX("linux", "so"), MAC("darwin", "dylib"), SOLARIS("solaris", "so");
        public final String name, libExtension;

        private OS(String name, String libExtension) {
            this.name = name;
            this.libExtension = libExtension;
        }
    }

    private static String arch() {
        return System.getProperty("os.arch");
    }

    private static OS os() {
        String osName = System.getProperty("os.name");
        if (osName.contains("Linux")) {
            return OS.LINUX;
        } else if (osName.contains("Mac")) {
            return OS.MAC;
        } else if (osName.contains("Windows")) {
            return OS.WINDOWS;
        } else if (osName.contains("Solaris") || osName.contains("SunOS")) {
            return OS.SOLARIS;
        } else {
            throw new UnsupportedOperationException("hadoop-4mc: Unsupported operating system: "
                    + osName);
        }
    }

    private static String resourceName() {
        OS os = os();
        String packagePrefix = FourMcNativeCodeLoader.class.getPackage().getName().replace('.', '/');
        return "/" + packagePrefix + "/" + os.name + "/" + arch() + "/libhadoop-4mc." + os.libExtension;
    }

    private static boolean useBinariesOnLibPath() {
        return Boolean.getBoolean(USE_BINARIES_ON_LIB_PATH);
    }

    private static synchronized void loadLibrary() {
        if (nativeLibraryLoaded) {
            LOG.info("hadoop-4mc: native library is already loaded");
            return;
        }

        if (useBinariesOnLibPath()) {
            try {
                System.loadLibrary("hadoop-4mc");
                nativeLibraryLoaded = true;
                LOG.info("hadoop-4mc: loaded native library (lib-path)");
            }
            catch (Exception e) {
                LOG.error("hadoop-4mc: cannot load native library (lib-path): ", e);
            }
            return;
        }

        // unpack and use embedded libraries

        String resourceName = resourceName();
        InputStream is = FourMcNativeCodeLoader.class.getResourceAsStream(resourceName);
        if (is == null) {
            throw new UnsupportedOperationException("Unsupported OS/arch, cannot find " + resourceName + ". Please try building from source.");
        }
        File tempLib;
        try {
            tempLib = File.createTempFile("libhadoop-4mc", "." + os().libExtension);
            // copy to tempLib
            FileOutputStream out = new FileOutputStream(tempLib);
            try {
                byte[] buf = new byte[4096];
                while (true) {
                    int read = is.read(buf);
                    if (read == -1) {
                        break;
                    }
                    out.write(buf, 0, read);
                }
                try {
                    out.close();
                    out = null;
                } catch (IOException e) {
                    // ignore
                }
                System.load(tempLib.getAbsolutePath());
                nativeLibraryLoaded = true;
                LOG.info("hadoop-4mc: loaded native library (embedded)");
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException e) {
                    // ignore
                }
                if (tempLib.exists()) {
                    if (!nativeLibraryLoaded) {
                        tempLib.delete();
                    } else {
                        tempLib.deleteOnExit();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("hadoop-4mc: cannot load native library  (embedded): ", e);
        }
    }


    static {
        loadLibrary();
    }

    public static boolean isNativeCodeLoaded() {
        return nativeLibraryLoaded;
    }

}

