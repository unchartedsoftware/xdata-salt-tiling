/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
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

package software.uncharted.xdata.tiling.config

import org.scalatest.FunSpec
import com.typesafe.config.ConfigFactory

class ConfigParserTest extends FunSpec with ConfigParser {

  describe("#ConfigParserTest") {

    it ("should return values from the supplied config") {
      val configKey = "testConfig"

      val sentence = "hello there"

      val otherConfig = ConfigFactory.parseString(
        s"""otherConfig = "testValue"""".stripMargin)

      val config = ConfigFactory.parseString(
        s"""testConfig.keyZero { otherConfig = "testValue" }
           |testConfig.keyOne = "hello world!"
           |testConfig.keyTwo = 23
           |testConfig.keyThree = 1.7
           |testConfig.keyFour = true
           |testConfig.keyFive = [$sentence, "bonjour"]
           |testConfig.keySix = [25]
           |testConfig.keySeven = [1.9, 2.0]
           |testConfig.keyEight = $sentence
           |testConfig.keyNine = 20
           |testConfig.keyTen = 1.8
           |testConfig.keyEleven = true
        """.stripMargin).resolve()

      val testConfig = config.getConfig(configKey)

      val configOpt = getConfigOption(testConfig, "keyZero")
      val strOpt = getStringOption(testConfig, "keyOne")
      val intOpt = getIntOption(testConfig, "keyTwo")
      val doubleOpt = getDoubleOption(testConfig, "keyThree")
      val boolOpt = getBooleanOption(testConfig, "keyFour")
      val strList = getStringList(testConfig, "keyFive")
      val intList = getIntList(testConfig, "keySix")
      val doubleList = getDoubleList(testConfig, "keySeven")
      val strValue = getString(testConfig, "keyEight", "defaultValue")
      val intValue = getInt(testConfig, "keyNine", 0)
      val doubleVal = getDouble(testConfig, "keyTen", 0.0)
      val boolVal = getBoolean(testConfig, "keyEleven", false)

      assertResult(Some(otherConfig))(configOpt)
      assertResult(Some("hello world!"))(strOpt)
      assertResult(Some(23))(intOpt)
      assertResult(Some(1.7))(doubleOpt)
      assertResult(Some(true))(boolOpt)
      assertResult(Seq(sentence, "bonjour"))(strList)
      assertResult(Seq(25))(intList)
      assertResult(Seq(1.9, 2.0))(doubleList)
      assertResult("hello there")(strValue)
      assertResult(20)(intValue)
      assertResult(1.8)(doubleVal)
      assertResult(true)(boolVal)
    }

    it ("should return default values when a key isn't found a default is supplied") {
      val sampleConfig = ConfigFactory.empty()
      val strDefault = getString(sampleConfig, "stringKey", "defaultString")
      val intDefault = getInt(sampleConfig, "integerKey", 10)
      val doubleDefault = getDouble(sampleConfig, "doubleKey", 15.0)
      val boolDefault = getBoolean(sampleConfig, "boolKey", false)

      assertResult("defaultString")(strDefault)
      assertResult(10)(intDefault)
      assertResult(15.0)(doubleDefault)
      assertResult(false)(boolDefault)
    }

    it ("should return empty sequences when a key isn't found") {
      val sampleConfig = ConfigFactory.empty()
      val emptyStrList = getStringList(sampleConfig, "stringListKey")
      val emptyIntList = getIntList(sampleConfig, "intListKey")
      val emptyDoubleList = getDoubleList(sampleConfig, "doubleListKey")

      assertResult(Seq[Double]())(emptyStrList)
      assertResult(Seq[Int]())(emptyIntList)
      assertResult(Seq[Double]())(emptyDoubleList)
    }

  }

}
