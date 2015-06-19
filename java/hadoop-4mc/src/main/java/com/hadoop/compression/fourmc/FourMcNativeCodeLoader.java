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
package com.hadoop.compression.fourmc;

import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FourMcNativeCodeLoader {
    private static final Log LOG = LogFactory.getLog(FourMcNativeCodeLoader.class);
    private static boolean nativeLibraryLoaded = false;

    static {
        try {
            //try to load the lib
            System.loadLibrary("hadoop-4mc");
            nativeLibraryLoaded = true;
            LOG.info("Loaded native hadoop-4mc library");
        } catch (Throwable t) {
            LOG.warn("Could not load native hadoop-4mc library", t);
            LOG.info("Trying loading from LD_LIBRARY_PATH ...");
            
            //try to load the lib from LD_LIBRARY_PATH if library is not found in java.library.path
            //helps application loading the libraries using distributed cache in Hadoop.
            String lib = "hadoop-4mc";
            String ld_lib_path = System.getenv("LD_LIBRARY_PATH");
            String[] paths = ld_lib_path.split(":");
            for(int i=0; i<paths.length; i++) {
               String p = paths[i];
               File x = new File(p, "lib" + lib + ".so");
               if (x.exists()) {
                  System.load(x.getAbsolutePath());
                  nativeLibraryLoaded = true;
                  break;
               }
            }
            if(nativeLibraryLoaded) {
                LOG.info("Loaded native hadoop-4mc library");
            } else {
                 LOG.error("Could not load native hadoop-4mc library");
            }
        }
    }

    public static boolean isNativeCodeLoaded() {
        return nativeLibraryLoaded;
    }

}
