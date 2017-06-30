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

package software.uncharted.salt.contrib.projection.geometry

import org.scalatest.FunSuite
import software.uncharted.salt.contrib.projection.MercatorTileProjection2D

class MercatorTileProjection2DTestSuite extends FunSuite {
  test("tile/bin to universal coordinates, y not flipped") {
    val t = new TestableMercatorSegmentProjection(false)

    assert((0, 0) === t.tb2ub((4, 0, 0), (0, 0), (3, 3)))
    assert((3, 0) === t.tb2ub((4, 0, 0), (3, 0), (3, 3)))
    assert((0, 3) === t.tb2ub((4, 0, 0), (0, 3), (3, 3)))
    assert((3, 3) === t.tb2ub((4, 0, 0), (3, 3), (3, 3)))

    assert((60, 60) === t.tb2ub((4, 15, 15), (0, 0), (3, 3)))
    assert((60, 63) === t.tb2ub((4, 15, 15), (0, 3), (3, 3)))
    assert((63, 60) === t.tb2ub((4, 15, 15), (3, 0), (3, 3)))
    assert((63, 63) === t.tb2ub((4, 15, 15), (3, 3), (3, 3)))
  }

  test("tile/bin to universal coordinates, y flipped") {
    val t = new TestableMercatorSegmentProjection(true)

    assert((0, 60) === t.tb2ub((4, 0, 0), (0, 0), (3, 3)))
    assert((0, 63) === t.tb2ub((4, 0, 0), (0, 3), (3, 3)))
    assert((3, 60) === t.tb2ub((4, 0, 0), (3, 0), (3, 3)))
    assert((3, 63) === t.tb2ub((4, 0, 0), (3, 3), (3, 3)))

    assert((60, 0) === t.tb2ub((4, 15, 15), (0, 0), (3, 3)))
    assert((60, 3) === t.tb2ub((4, 15, 15), (0, 3), (3, 3)))
    assert((63, 0) === t.tb2ub((4, 15, 15), (3, 0), (3, 3)))
    assert((63, 3) === t.tb2ub((4, 15, 15), (3, 3), (3, 3)))
  }

  test("universal bin to tile/bin, y not flipped") {
    val t = new TestableMercatorSegmentProjection(false)

    assert(((4, 0, 0), (0, 0)) === t.ub2tb(4, (0, 0), (3, 3)))
    assert(((4, 0, 0), (3, 0)) === t.ub2tb(4, (3, 0), (3, 3)))
    assert(((4, 0, 0), (0, 3)) === t.ub2tb(4, (0, 3), (3, 3)))
    assert(((4, 0, 0), (3, 3)) === t.ub2tb(4, (3, 3), (3, 3)))

    assert(((4, 15, 15), (0, 0)) === t.ub2tb(4, (60, 60), (3, 3)))
    assert(((4, 15, 15), (0, 3)) === t.ub2tb(4, (60, 63), (3, 3)))
    assert(((4, 15, 15), (3, 0)) === t.ub2tb(4, (63, 60), (3, 3)))
    assert(((4, 15, 15), (3, 3)) === t.ub2tb(4, (63, 63), (3, 3)))
  }

  test("universal bin to tile/bin, y flipped") {
    val t = new TestableMercatorSegmentProjection(true)

    assert(((4, 0, 0), (0, 0)) === t.ub2tb(4, (0, 60), (3, 3)))
    assert(((4, 0, 0), (0, 3)) === t.ub2tb(4, (0, 63), (3, 3)))
    assert(((4, 0, 0), (3, 0)) === t.ub2tb(4, (3, 60), (3, 3)))
    assert(((4, 0, 0), (3, 3)) === t.ub2tb(4, (3, 63), (3, 3)))

    assert(((4, 15, 15), (0, 0)) === t.ub2tb(4, (60, 0), (3, 3)))
    assert(((4, 15, 15), (0, 3)) === t.ub2tb(4, (60, 3), (3, 3)))
    assert(((4, 15, 15), (3, 0)) === t.ub2tb(4, (63, 0), (3, 3)))
    assert(((4, 15, 15), (3, 3)) === t.ub2tb(4, (63, 3), (3, 3)))
  }

  test("tile/bin round trip through universal bin") {
    def roundTrip (t: TestableMercatorSegmentProjection, z: Int, x: Int, y: Int): (Int, Int) = {
      val (tile, bin) = t.ub2tb(z, (x, y), (3, 3))
      t.tb2ub(tile, bin, (3, 3))
    }

    val tt = new TestableMercatorSegmentProjection(true)
    val tf = new TestableMercatorSegmentProjection(false)
    for (z <- 0 to 4) {
      val max = 1 << z
      for (x <- 0 until max; y <- 0 until max) {
        assert((x, y) === roundTrip(tt, z, x, y))
        assert((x, y) === roundTrip(tf, z, x, y))
      }
    }
  }

  test("Universal bin tile bounds") {
    val tt = new TestableMercatorSegmentProjection(true)
    val tf = new TestableMercatorSegmentProjection(false)

    for (x <- 0 to 15; y <- 0 to 15) {
      val (minBinNorm, maxBinNorm) = tf.ubtf((4, x, y), (3, 3))
      val (minBinTMS, maxBinTMS) = tt.ubtf((4, x, 15 - y), (3, 3))
      assert(minBinNorm === minBinTMS)
      assert(maxBinNorm === maxBinTMS)
      assert(minBinNorm._1 === 4 * x)
      assert(maxBinNorm._1 === 4 * x + 3)
      assert(minBinNorm._2 === 4 * y)
      assert(maxBinNorm._2 === 4 * y + 3)
    }
  }
}

// Just a test object to expose the protected functions in our base projection class
class TestableMercatorSegmentProjection (tms: Boolean) extends MercatorTileProjection2D[Any, Any]((0.0, 0.0), (16.0, 16.0), tms) {
  def tb2ub(tile: (Int, Int, Int),
            bin: (Int, Int),
            maxBin: (Int, Int)): (Int, Int) =
    tileBinIndexToUniversalBinIndex(tile, bin, maxBin)

  def ub2tb(z: Int, ub: (Int, Int), maxBin: (Int, Int)): ((Int, Int, Int), (Int, Int)) =
    universalBinIndexToTileIndex(z, ub, maxBin)

  def ubtf (tile: (Int, Int, Int), maxBin: (Int, Int)) = universalBinTileBounds(tile, maxBin)

  // Just stub implementations of projection functions - we aren't testing those.
  override def project(dc: Option[Any], maxBin: Any): Option[Seq[((Int, Int, Int), Any)]] = None
  override def binTo1D(bin: Any, maxBin: Any): Int = 0
  override def binFrom1D(index: Int, maxBin: Any): Any = 0
}
