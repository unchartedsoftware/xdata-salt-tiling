//// spark-shell --master yarn-client  --executor-cores 4  --num-executors 3  --executor-memory 5G  --driver-memory 3g --conf spark.kryoserializer.buffer=256 --conf spark.kryoserializer.buffer.max=512 --jars target/btm-1.0-SNAPSHOT.jar
//
//// val bdp = new RunBDPParallel(sc)
//// val hdfspath = "/xdata/data/twitter/isil-keywords/2016-03/*"
//// val outpath = "/home/chagerman/Topic_Modeling/BTM/Output/"
//// bdp.main(hdfspath, outpath)
//
//
//
//
//import java.io.{PrintWriter, File}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.broadcast.Broadcast
//
//import com.uncharted.btm._
//import com.uncharted.btm.BDPParallel
//import com.uncharted.btm.BTMUtil
//import com.uncharted.btm.Coherence
//import com.uncharted.btm.DatePartitioner
//import com.uncharted.btm.TFIDF
//import com.uncharted.btm.WordDict
//
//
//class RunBDPParallel(context: org.apache.spark.SparkContext) {
//  val sc = context
//
//  //  --------------------   Utility Functions   --------------------  //
//  /** Load a sample of tweets I previously preprocessed for experiments. Schema => (YMD, id, text)
//   */
//  def loadCleanTweets(path: String, dates: Array[String], caIdx: Int = 0, idIdx: Int = 1, textIdx: Int = 2) = {
//    val rdd = sc.textFile(path)
//      .map(_.split("\t"))
//      .filter(x => x.length > textIdx)
//      .filter(x => dates contains x(caIdx))
//    rdd
//  }
//
//
//  /*
//  val path = "/xdata/data/twitter/isil-keywords/2016-09/isil_keywords.2016090 "
//  val dates = Array("2016-09-03", "2016-09-04", "2016-09-05")
//  val bdp = RunBDPParallel(sc)
//  val rdd = bdp.loadTSVTweets(path, dates, 1, 0, 6)
//
//   */
//  def loadTSVTweets(path: String, dates: Array[String], caIdx: Int = 0, idIdx: Int = 1, textIdx: Int = 2) = {
//    val rdd = sc.textFile(path)
//      .map(_.split("\t"))
//      .filter(x => x.length > textIdx)
//      .map(x => Array(x(caIdx), x(idIdx), x(textIdx) ))
//      .map{ case Array(d, i, t) => Array(BTMUtil.ca2ymd(d), i, t) }
//      .filter{ case Array( d, i, t) => dates contains d }
//    rdd
//  }
//
//
//  def castResults(parts: Array[Array[Any]]) = {
//    val cparts = parts.map { p =>
//      val date = p(0).toString
//      val topic_dist = p(1).asInstanceOf[Array[(Double, Seq[String])]]
//      val theta = p(2).asInstanceOf[Array[Double]]
//      val phi = p(3).asInstanceOf[Array[Double]]
//      val nzMap = p(4).asInstanceOf[scala.collection.mutable.HashMap[Int, Int]].toMap
//      val m = p(5).asInstanceOf[Int]
//      val duration = p(6).asInstanceOf[Double]
//      (date, topic_dist, theta, phi, nzMap, m, duration)
//    }
//    cparts
//  }
//
//
//  def output_results(topic_dist: Array[(Double, Seq[String])], nzMap: scala.collection.immutable.Map[Int, Int], theta: Array[Double], phi: Array[Double], date: String = "---", iterN: Int, m: Int, alpha: Double, beta: Double, duration: Double, outdir: String, cs: Array[Double] = Array(Double.NaN), avg_cs: Double = Double.NaN) = {
//    def auto_label2(topic_dist: Array[(Double, Seq[String])]): Array[(Double, Seq[String], Seq[String])] = {
//      // extract top 3 hashtags from each topic, append these 'labels' to each row
//      // TODO function unnecessary
//      def find_labels(tp: Seq[String]): Seq[String] = {
//        val hashtags = tp.filter(_.startsWith("#")).take(3)
//        val terms = tp.filterNot(_.startsWith("#")).take(3)
//        val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take (3)
//        labels
//      }
//      val labeled = topic_dist.map { case (theta, tpcs) => (theta, find_labels(tpcs), tpcs) }
//      labeled
//    }
//    // TODO unnest
//    def write_topics(labeled_topic_dist: Array[(Double, Seq[String], Seq[String])], nzMap: scala.collection.immutable.Map[Int, Int], date: String, iterN: Int, m: Int, alpha: Double, beta: Double, duration: String, outfile: String) = {
//      val out = new PrintWriter(new File(outfile))
//      val k = labeled_topic_dist.size
//      out.println(s"# Date: ${date}\talpha: ${alpha}\tbeta: ${beta}\titerN: ${iterN}\tM: ${m}\tK: ${k}")
//      out.println(s"# Running time:\t${duration} min.")
//      out.println(s"Average Coherence Score: ${avg_cs}")
//      out.println(s"Coherence scores: " + cs.mkString(", "))
//      out.println("#" + "-" * 80)
//      out.println("#Z\tCount\tp(z)\t\t\tTop terms descending")
//      out.println("#" + "-" * 80)
//      labeled_topic_dist.zipWithIndex.map { case (td, i) => i + "\t" + nzMap(i) + "\t" + td._1 + "\t" + td._2.mkString(", ") + "\t->\t" + td._3.take(20).mkString(", ") } foreach {
//        out.println
//      }
//      out.close
//    }
//    println(s"Writing results to directory ${outdir}")
//    val k = topic_dist.size
//    val labeled_topic_dist = auto_label2(topic_dist)
//    val durationStr = "%.4f".format(duration)
//    val outfile1 = outdir + s"topics_${date}.txt"
//    write_topics(labeled_topic_dist, nzMap, date, iterN, m, alpha, beta, durationStr, outfile1)
//  }
//
//
//
//  //  --------------------   set up parameters & load data   --------------------  //
//  def setup(hdfspath: String) = {
//    // INIALIZE PARAMETERS
//    val lang = "en"
//    val iterN = 150
//    val alpha = 1 / Math.E
//    val eta = 0.01
//    var k = 2
//
//    // LM INPUT DATA
//    val swfiles = List("/Stopwords/stopwords_all_en.v2.txt",  "/Stopwords/stopwords_ar.txt",  "/Stopwords/stopwords_html_tags.txt")
//    val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
//    val stopwords_bcst = sc.broadcast(stopwords)
//
//    // dates +/- 1 day around significant events: Brussel's Attack
//    val dates = Array("2016-03-21", "2016-03-22", "2016-03-23")
//
//    // load data (pre-cleaned)
////    val caIdx = 0       // 'created_at' index
////    val idIdx = 1       // twitter_id index
////    val textIdx = 2     // text index
////    val rdd = loadCleanTweets(hdfspath, dates, caIdx, idIdx, textIdx)
//
//    // load data
//    val c = 1     // 'created_at' index
//    val i = 0     // twitter_id index
//    val t = 6     // text index
//    val rdd = loadTSVTweets(hdfspath, dates, 1, 0, 6)
//
//
//    (rdd, dates, stopwords_bcst, iterN, k, alpha, eta)
//  }
//
//
//
//
//  def run(rdd: RDD[Array[String]], dates: Array[String], stopwords_bcst: Broadcast[Set[String]], iterN: Int, k: Int, alpha: Double, eta: Double, outdir: String, weighted: Boolean = false, tfidf_bcst: Broadcast[Array[(String, String, Double)]] = null) = {
//    // group records by date
//    val kvrdd = BDPParallel.keyvalueRDD(rdd)
//    // partition data by date
//    val partitions = kvrdd.partitionBy(new DatePartitioner(dates))
//    // run BTM on each partition
//    val parts = partitions.mapPartitions { iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, tfidf_bcst) }.collect
//
//    // Compute Coherence Scores for each of the topic distibutions
//    // define number of top words to use to compute coherence score
//    val topT = 10
//    val cparts = castResults(parts)
//    cparts.foreach { cp =>
//      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
//      val topic_terms = topic_dist.map(x => x._2.toArray)
//      val textrdd = rdd.filter(x => x(0) == date).map(x => x(2))
//      // takes a long time to calculate Coherence. Uncomment to enable
////      val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
//
//      // Save results to a local file
////      output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)         // n.b. outputing coherence scores as well
//      output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir)
//    }
//  }
//
//
//
//
//  def main(hdfspath: String, outdir: String, tfidf_path:String="") = {
//    var weighted = false
//    if (tfidf_path != "") weighted = true
//
//      val (rdd, dates, stopwords_bcst, iterN, k, alpha, eta) = setup(hdfspath)
//
//      val tfidf_bcst = if (weighted) {
//        val tfidf_array = TFIDF.loadTfidf(tfidf_path, dates)
//        sc.broadcast(tfidf_array)
//      }
//      else null
//
//      run(rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir, weighted, tfidf_bcst)
//
//    }
//
//
//
//}
