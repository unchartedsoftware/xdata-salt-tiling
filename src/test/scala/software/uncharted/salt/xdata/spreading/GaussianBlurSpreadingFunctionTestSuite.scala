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

package software.uncharted.salt.xdata.spreading

import org.scalatest.FunSuite

class GaussianBlurSpreadingFunctionTestSuite extends FunSuite {

  test("Test gaussian blur spreader function") {
    val radius = 1
    val sigma = 3
    val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
    val spreader = new GaussianBlurSpreadingFunction2D(radius, sigma, (255, 255), tms = true)

    val actual = spreader.spread(Traversable(
      ((1, 1, 0), (5, 5)),
      ((1, 1, 0), (6, 5))
    ), Some(1.0))

    val expected = Traversable(
      ((1, 1, 0), (4, 4), Some(kernel(2)(2))),
      ((1, 1, 0), (5, 4), Some(kernel(2)(1) + kernel(2)(2))),
      ((1, 1, 0), (6, 4), Some(kernel(2)(0) + kernel(2)(1))),
      ((1, 1, 0), (7, 4), Some(kernel(2)(0))),
      ((1, 1, 0), (4, 5), Some(kernel(1)(2))),
      ((1, 1, 0), (5, 5), Some(kernel(1)(1) + kernel(1)(2))),
      ((1, 1, 0), (6, 5), Some(kernel(1)(0) + kernel(1)(1))),
      ((1, 1, 0), (7, 5), Some(kernel(1)(0))),
      ((1, 1, 0), (4, 6), Some(kernel(0)(2))),
      ((1, 1, 0), (5, 6), Some(kernel(0)(1) + kernel(0)(2))),
      ((1, 1, 0), (6, 6), Some(kernel(0)(0) + kernel(0)(1))),
      ((1, 1, 0), (7, 6), Some(kernel(0)(0)))
    )

    assert(expected.toSet === actual.toSet)
  }

  test("Test gaussian blur spreader function spills into neighboring tiles properly") {
    val radius = 1
    val sigma = 3
    val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
    val spreader = new GaussianBlurSpreadingFunction2D(radius, sigma, (255, 255), tms = true)

    val actual = spreader.spread(Traversable(
      ((3, 1, 1), (255, 255))
    ), Some(1.0))

    val expected = Traversable(
      ((3, 1, 1), (254, 254), Some(kernel(2)(2))),
      ((3, 1, 1), (255, 254), Some(kernel(2)(1))),
      ((3, 2, 1), (0, 254), Some(kernel(2)(0))),
      ((3, 1, 1), (254, 255), Some(kernel(1)(2))),
      ((3, 1, 1), (255, 255), Some(kernel(1)(1))),
      ((3, 2, 1), (0, 255), Some(kernel(1)(0))),
      ((3, 1, 0), (254, 0), Some(kernel(0)(2))),
      ((3, 1, 0), (255, 0), Some(kernel(0)(1))),
      ((3, 2, 0), (0, 0), Some(kernel(0)(0)))
    )

    assert(expected.toSet === actual.toSet)
  }

  test("Test GaussianBlurSpreadingFunction3D") {
    val radius = 1
    val sigma = 3

    //Gaussian function that makes blurring mask
    val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)

    //input is a single pixel and its initial tile/bin coordinates
    val tileCoords = (3, 1, 1)
    val binCoords = (1, 1, 1)
    val coordsTraversable = Traversable((tileCoords, binCoords))

    //original value of single pixel
    val value = Some(1.0)

    val maxBinDimensions = (1, 1)
    val spreader = new GaussianBlurSpreadingFunction3D(radius, sigma, maxBinDimensions, tms = true)
    val result = spreader.spread(coordsTraversable, value)

    //bin values is initial value multiplied by corresponding kernel value
    val expected = Traversable(
      ((3, 1, 1), (0, 0, 1), Some(kernel(0)(0))),
      ((3, 1, 1), (0, 1, 1), Some(kernel(1)(0))),
      ((3, 1, 1), (1, 1, 1), Some(kernel(1)(1))),
      ((3, 1, 1), (1, 0, 1), Some(kernel(0)(1))),
      ((3, 2, 1), (0, 0, 1), Some(kernel(0)(2))),
      ((3, 2, 1), (0, 1, 1), Some(kernel(1)(2))),
      ((3, 1, 0), (0, 0, 1), Some(kernel(2)(0))),
      ((3, 1, 0), (1, 0, 1), Some(kernel(2)(1))),
      ((3, 2, 0), (0, 0, 1), Some(kernel(2)(2)))
    )

    assert(expected.toSet === result.toSet)
  }

  test("Getting coordinates correctly when tms is false") {
    val radius = 1
    val sigma = 3

    //Gaussian function that makes blurring mask
    val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)

    //input is a single pixel and its initial tile/bin coordinates
    val tileCoords = (3, 1, 1)
    val binCoords = (1, 1, 1)
    val coordsTraversable = Traversable((tileCoords, binCoords))

    //original value of single pixel
    val value = Some(1.0)

    val maxBinDimensions = (1, 1)
    val spreader = new GaussianBlurSpreadingFunction3D(radius, sigma, maxBinDimensions, tms = false)
    val result = spreader.spread(coordsTraversable, value)

    //bin values is initial value multiplied by corresponding kernel value
    val expected = Traversable(
      ((3, 1, 1), (0, 0, 1), Some(kernel(0)(0))),
      ((3, 1, 1), (0, 1, 1), Some(kernel(1)(0))),
      ((3, 1, 1), (1, 1, 1), Some(kernel(1)(1))),
      ((3, 1, 1), (1, 0, 1), Some(kernel(0)(1))),
      ((3, 2, 1), (0, 0, 1), Some(kernel(0)(2))),
      ((3, 2, 1), (0, 1, 1), Some(kernel(1)(2))),
      ((3, 1, 2), (0, 0, 1), Some(kernel(2)(0))),
      ((3, 1, 2), (1, 0, 1), Some(kernel(2)(1))),
      ((3, 2, 2), (0, 0, 1), Some(kernel(2)(2)))
    )

    assert(expected.toSet === result.toSet)
  }

}
