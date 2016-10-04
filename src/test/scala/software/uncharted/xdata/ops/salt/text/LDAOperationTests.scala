package software.uncharted.xdata.ops.salt.text

import software.uncharted.xdata.spark.SparkFunSpec
import software.uncharted.xdata.ops.util.BasicOperations

/**
  * Created by nkronenfeld on 03/10/16.
  */
class LDAOperationTests extends SparkFunSpec {
  import BasicOperations._

  describe("Word determination") {
    it("should break a text apart into words properly, including dealing with contractions") {
      val words =
        """She said, 'The quick brown fox doesn't ever jump over the lazy
          |dog.  I say it won't, it doesn't, and it didn't, and I'm sticking
          |to that!""".split(LDAOp.notWord).toList
      assert(List(
        "She", "said", "The", "quick", "brown", "fox", "doesn't", "ever", "jump", "over",
        "the", "lazy", "dog", "I", "say", "it", "won't", "it", "doesn't", "and", "it",
        "didn't", "and", "I'm", "sticking", "to", "that") === words)
    }
  }

  describe("#lda") {
    import LDAOp._
    it("should return a proper dataframe, with the proper elements in it") {
      val data = toDataFrame(sqlc)(sc.parallelize(List(
        new LDATestData(0L, "aaa bbb ccc ddd eee"),
        new LDATestData(1L, "aaa aaa bbb bbb ccc"),
        new LDATestData(2L, "aaa bbb bbb ccc ccc"),
        new LDATestData(3L, "aaa eee fff fff ggg "),
        new LDATestData(4L, "aaa aaa fff ggg ggg"),
        new LDATestData(5L, "aaa aaa aaa fff eee"),
        new LDATestData(6L, "aaa bbb ccc ddd eee fff ggg")
      )))

      val results = lda("index", "text", 2, 3, 2)(data)

      results.schema.fields.foreach(println)
    }
  }
}

case class LDATestData (index: Long, text: String)
