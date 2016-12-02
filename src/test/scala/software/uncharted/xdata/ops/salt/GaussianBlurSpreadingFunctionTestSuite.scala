package software.uncharted.xdata.ops.salt

import org.scalatest.FunSuite

class GaussianBlurSpreadingFunctionTestSuite extends FunSuite{
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
}
