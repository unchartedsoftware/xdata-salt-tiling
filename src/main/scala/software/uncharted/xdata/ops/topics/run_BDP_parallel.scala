// spark-shell --master yarn-client  --executor-cores 4  --num-executors 3  --executor-memory 5G  --driver-memory 3g --conf spark.kryoserializer.buffer=256 --conf spark.kryoserializer.buffer.max=512 --jars /home/chagerman/target/btm-1.0-SNAPSHOT.jar


import java.io._
import com.uncharted.btm._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast._


class RunBDPParallel(context: org.apache.spark.SparkContext) {
  val sc = context

  //  --------------------   Utility Functions   --------------------  //
  def loadRDDDates(path: String, dates: Array[String], caIdx: Int = 0, idIdx: Int = 1, textIdx: Int = 2) = {
    val rdd = sc.textFile(path)
      .map(_.split("\t"))
      .filter(x => x.length > textIdx)
      .filter(x => dates contains x(caIdx))
    rdd
  }


  def castResults(parts: Array[Array[Any]]) = {
    val cparts = parts.map { p =>
      val date = p(0).toString
      val topic_dist = p(1).asInstanceOf[Array[(Double, Seq[String])]]
      val theta = p(2).asInstanceOf[Array[Double]]
      val phi = p(3).asInstanceOf[Array[Double]]
      val nzMap = p(4).asInstanceOf[scala.collection.mutable.HashMap[Int, Int]].toMap
      val m = p(5).asInstanceOf[Int]
      val duration = p(6).asInstanceOf[Double]
      (date, topic_dist, theta, phi, nzMap, m, duration)
    }
    cparts
  }


  def output_results(topic_dist: Array[(Double, Seq[String])], nzMap: scala.collection.immutable.Map[Int, Int], theta: Array[Double], phi: Array[Double], date: String = "---", iterN: Int, m: Int, alpha: Double, beta: Double, duration: Double, outdir: String, cs: Array[Double] = Array(Double.NaN), avg_cs: Double = Double.NaN) = {
    def auto_label2(topic_dist: Array[(Double, Seq[String])]): Array[(Double, Seq[String], Seq[String])] = {
      // extract top 3 hashtags from each topic, append these 'labels' to each row
      def find_labels(tp: Seq[String]): Seq[String] = {
        val hashtags = tp.filter(_.startsWith("#")).take(3)
        val terms = tp.filterNot(_.startsWith("#")).take(3)
        val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take (3)
        labels
      }
      val labeled = topic_dist.map { case (theta, tpcs) => (theta, find_labels(tpcs), tpcs) }
      labeled
    }
    def write_topics(labeled_topic_dist: Array[(Double, Seq[String], Seq[String])], nzMap: scala.collection.immutable.Map[Int, Int], date: String, iterN: Int, m: Int, alpha: Double, beta: Double, duration: String, outfile: String) = {
      val out = new PrintWriter(new File(outfile))
      val k = labeled_topic_dist.size
      out.println(s"# Date: ${date}\talpha: ${alpha}\tbeta: ${beta}\titerN: ${iterN}\tM: ${m}\tK: ${k}")
      out.println(s"# Running time:\t${duration} min.")
      out.println(s"Average Coherence Score: ${avg_cs}")
      out.println(s"Coherence scores: " + cs.mkString(", "))
      out.println("#" + "-" * 80)
      out.println("#Z\tCount\tp(z)\t\t\tTop terms descending")
      out.println("#" + "-" * 80)
      labeled_topic_dist.zipWithIndex.map { case (td, i) => i + "\t" + nzMap(i) + "\t" + td._1 + "\t" + td._2.mkString(", ") + "\t->\t" + td._3.take(20).mkString(", ") } foreach {
        out.println
      }
      out.close
    }
    println(s"Writing results to directory ${outdir}")
    val k = topic_dist.size
    val labeled_topic_dist = auto_label2(topic_dist)
    val durationStr = "%.4f".format(duration)
    val outfile1 = outdir + s"topics_${date}.txt"
    write_topics(labeled_topic_dist, nzMap, date, iterN, m, alpha, beta, durationStr, outfile1)
  }



  //  --------------------   set up parameters & load data   --------------------  //
  def setup() = {
    // INIALIZE PARAMETERS
    val lang = "en"
    val iterN = 150
    val alpha = 1 / Math.E
    val eta = 0.01
    var k = 2

    // INITIALIZE DATA PATHS
    val outdir = s"/home/chagerman/Topic_Modeling/BTM/OUTPUT/parallel_BDP2/"
    val basedir = "/home/chagerman/Topic_Modeling/BTM/Input/" // nb. basedir contains LM input such as stopwords, word_dict, etc
    val hdfspath = "/user/chagerman/BTM_eval_data/*"

    // LM INPUT DATA
    val datadir = "/home/chagerman/data/"
    val swfiles = List(datadir + "STOPWORDS/stopwords_all_en.v2.txt", datadir + "STOPWORDS/stopwords_ar.txt", datadir + "STOPWORDS/stopwords_html_tags.txt")
    val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
    val stopwords_bcst = sc.broadcast(stopwords)

    // dates +/- 1 day around significant events
    val dates1 = Array("2015-01-06", "2015-01-07", "2015-01-08")
    val dates2 = Array("2015-11-12", "2015-11-13", "2015-11-14", "2015-12-01", "2015-12-02", "2015-12-03")
    val dates3 = Array("2016-03-21", "2016-03-22", "2016-03-23")
    val dates = dates2 ++ dates3

    // load data
    val caIdx = 0
    val idIdx = 1
    val textIdx = 2
    val rdd = loadRDDDates(hdfspath, dates, caIdx, idIdx, textIdx)

    (rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir)
  }




  def run(rdd: RDD[Array[String]], dates: Array[String], stopwords_bcst: Broadcast[Set[String]], iterN: Int, k: Int, alpha: Double, eta: Double, outdir: String, weighted: Boolean = false, tfidf_path: String = "") = {
    // group records by date
    val kvrdd = BDPParallel.keyvalueRDD(rdd)
    // partition data by date
    val partitions = kvrdd.partitionBy(new DatePartitioner(dates))
    // run BTM on each partition
    val parts = partitions.mapPartitions { iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, tfidf_path) }.collect

    // Compute Coherence Scores for each of the topic distibutions
    // define number of top words to use to compute coherence score
    val topT = 10
    val cparts = castResults(parts)
    cparts.foreach { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
      val topic_terms = topic_dist.map(x => x._2.toArray)
      val textrdd = rdd.filter(x => x(0) == date).map(x => x(2))
      val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)

      // Save results to a local file
      output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)
    }
  }



  def main = {
    val (rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir) = setup()
    val weighted = false
    val tfidf_path = ""
    run(rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir, weighted, tfidf_path)
  }

}


