package software.uncharted.xdata.ops.salt.text

import software.uncharted.xdata.spark.SparkFunSpec

/**
  * Created by nkronenfeld on 03/10/16.
  */
class LDAOperationTests extends SparkFunSpec {
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
}
