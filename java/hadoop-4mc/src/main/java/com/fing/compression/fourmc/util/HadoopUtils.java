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
package com.fing.compression.fourmc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * Borrowed from smart EB framework, leverages reflection to be compatible with both hadoop 1.x and 2.x.
 */
public class HadoopUtils {
  private static final boolean hadoopV2;

  private static final Method GET_CONFIGURATION;

  private static final String PACKAGE = "org.apache.hadoop.mapreduce";

  static {
    boolean v2 = true;
    try {
      // use the presence of JobContextImpl as a test for 2.x
      Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v2 = false;
    }
    hadoopV2 = v2;

    try {
      GET_CONFIGURATION = getMethod(".JobContext", "getConfiguration");
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
  }

  /**
   * Wrapper for Class.forName(className).getMethod(methodName, parmeterTypes);
   */
  private static Method getMethod(String className, String methodName, Class<?>... paramTypes)
      throws ClassNotFoundException, NoSuchMethodException {
    return Class.forName(className.startsWith(".") ? PACKAGE + className : className)
        .getMethod(methodName, paramTypes);
  }

  /**
   * Returns true whether the runtime hadoop version is 2.x, false otherwise.
   */
  public static boolean isVersion2x() {
    return hadoopV2;
  }

  private static Object invoke(Method method, Object obj, Object... args) {
    try {
      return method.invoke(obj, args);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    }
  }

  /**
   * Invokes getConfiguration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  public static Configuration getConfiguration(JobContext context) {
    return (Configuration)invoke(GET_CONFIGURATION, context);
  }

}
