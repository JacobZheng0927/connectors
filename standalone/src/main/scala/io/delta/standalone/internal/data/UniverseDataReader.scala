// scalastyle:off
/*
 * Copyright (c) 2018 Marcin Jakubowski
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * This file contains code from the parquet4s project (original license above).
 * It contains modifications, which are licensed as follows:
 */
// scalastyle:on

/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.data

import com.github.mjakubowski84.parquet4s._
import io.delta.standalone.types._
import java.util.{Locale, TimeZone}

private[internal] object UniverseDataReader {

  private val codecConf = ValueCodecConfiguration(TimeZone.getDefault)

  def decodeParquetVal(parquetVal: Value): Any = {
    var typeName = getParquetValTypeName(parquetVal)
    if (typeName.equalsIgnoreCase("binary")) {
      typeName = "string"
    }
    if (primitiveDecodeMap.contains(typeName)) {
      return primitiveDecodeMap(typeName).decode(parquetVal, codecConf)
    }
    if (primitiveNullableDecodeMap.contains(typeName)) {
      return primitiveNullableDecodeMap(typeName).decode(parquetVal, codecConf)
    }
    "null"
  }

  private def getParquetValTypeName(parquetVal: Value): String = {
    var tmp = parquetVal.getClass.getSimpleName
    tmp = stripSuffix(tmp, "$")
    tmp = stripSuffix(tmp, "Type")
    tmp = stripSuffix(tmp, "UDT")
    tmp = stripSuffix(tmp, "Value")
    tmp.toLowerCase(Locale.ROOT)
  }

  private def stripSuffix(orig: String, suffix: String): String = {
    if (null != orig && orig.endsWith(suffix)) return orig.substring(0, orig.length - suffix.length)
    orig
  }

  private val primitiveDecodeMap = Map(
    new IntegerType().getTypeName -> ValueCodec.intCodec,
    new LongType().getTypeName -> ValueCodec.longCodec,
    new ByteType().getTypeName -> ValueCodec.byteCodec,
    new ShortType().getTypeName -> ValueCodec.shortCodec,
    new BooleanType().getTypeName -> ValueCodec.booleanCodec,
    new FloatType().getTypeName -> ValueCodec.floatCodec,
    new DoubleType().getTypeName -> ValueCodec.doubleCodec
  )

  private val primitiveNullableDecodeMap = Map(
    new StringType().getTypeName -> ValueCodec.stringCodec,
    new BinaryType().getTypeName -> ValueCodec.arrayCodec[Byte, Array],
    new TimestampType().getTypeName -> ValueCodec.sqlTimestampCodec,
    new DateType().getTypeName -> ValueCodec.sqlDateCodec
  )

}
