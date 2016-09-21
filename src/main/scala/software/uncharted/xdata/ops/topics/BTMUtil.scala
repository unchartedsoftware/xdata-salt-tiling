/**
  * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */

package software.uncharted.xdata.ops.topics

import java.io._

import grizzled.slf4j.{Logger, Logging}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random


case class Biterm(biterm: (Int, Int), var z: Int) extends Serializable with Logging

object BTMUtil extends Serializable with Logging {

  /**
    * load biterms
    */
  def generateBitermsLocal(path: String, word_dict: Map[String, Int], stopwords: Set[String], k: Int, textIdx: Int = 0) = {
    // val biterms = Source.fromFile(path).getLines.map(text => BTMUtil.extractBitermsFromTextRandomK(text, word_dict, stopwords, k)).flatMap(x => x)
    val biterms = Source.fromFile(path).getLines.map(_.split("\t")).map(x => x(textIdx)).map(text => BTMUtil.extractBitermsFromTextRandomK(text, word_dict, stopwords, k)).flatMap(x => x)
    biterms.toArray
  }

  /**
   * Receive an array of tokens (cleaned words). Remove stopwords, out-of-vocabulary words
   * Return an array of word-ids corresponding to each word
   */
  def getWordIds(tokens: Array[String], word_dict: Map[String, Int], stopwords: Set[String]) = {
    tokens.filter(w => !(stopwords contains w))              // ignore words in stopwords
                        .map(word => word_dict.getOrElse(word, -1))         // get the word_id associated with word
                        .filter(x => x > -1)                                // ignore out-of-vocabulary words           // ToDo: should handled OOV words - save & output a list with counts?
  }


  // TODO split below sections into diff files: load, save model topics, etc
  // =============================  load data  ====================================================================
  def load_stopwordsLocal(spath: String) = {
    val swords = Source.fromFile(spath).getLines.map(w => TextUtil.cleanText(w.toLowerCase.trim) ).filter(_ != "").toSet
    swords
  }

  def loadWords(wordPath: String, wordDictPath: String, stopwordPath: String) = {
    info("Loading stopwords, vocab, word dictionary...\n")
    val words = Source.fromFile(wordPath).getLines.toArray
    val word_dict = Source.fromFile(wordDictPath).getLines.map(_.split("\t")).toArray.map(x => (x(0), x(1).toInt)).toMap
    val stopwords = Source.fromFile(stopwordPath).getLines.map(_.toLowerCase).toSet
    (words, word_dict, stopwords)
  }

  // def loadRDD(hdfspath: String, lang: String, inclusive: Boolean, langIdx: Int = 7 ) = {
  //     val rdd0 = sc.textFile(hdfspath).map(_.split("\t")).filter(x => x.length > 7)
  //     val rdd = inclusive match {
  //         case true => rdd0.filter(x => x(langIdx) == lang).cache                                     // filter for a specified language
  //         case false => rdd0.filter(x => x(langIdx) != "ar").filter(x => x(langIdx) != "en").cache    // exclude English and Arabic
  //     }
  //     rdd
  // }

  def getBiterms(d:Array[Int]):Iterator[(Int, Int)] = {
    d.toSeq.combinations(2).map { case Seq(w1, w2) =>
      if (w1 < w2) (w1, w2) else (w2, w1)
    }
  }

  def extractBitermsLocal(files: Seq[Array[String]], word_dict: Map[String, Int], stopwords: Set[String], idIdx: Int=0, textIdx: Int=1) = {
    val tokenized = files.map(x => (x(idIdx), TextUtil.cleanText(x(textIdx)).split("\\s+")) )
    val biterms = tokenized.map { r =>
      val (tid, tokens) = r
      val d = getWordIds(tokens, word_dict, stopwords)
      val b = getBiterms(d).toArray
      b }
    biterms.flatMap(x => x).map(x => Biterm(x, -1)).toArray
  }

  def extractBitermStringFromText(text: String, word_dict: Map[String, Int], stopwords: Set[String]) = {
    val tokens = TextUtil.cleanText(text).split("\\s+")
    val d = getWordIds(tokens, word_dict, stopwords)
    val b = getBiterms(d).toArray
    b.map { case (w1, w2) => w1 + "," + w2}.mkString("\t")          // output tab-separated biterms where biterms are comma-separated words
  }

  def extractBitermsFromText(text: String, word_dict: Map[String, Int], stopwords: Set[String]) = {
    val tokens = TextUtil.cleanText(text).split("\\s+")
    val d = getWordIds(tokens, word_dict, stopwords)
    val b = getBiterms(d).toArray
    b.map(x => Biterm(x, -1)).toSeq
  }

  def extractBitermsFromTextRandomK(text: String, word_dict: Map[String, Int], stopwords: Set[String], k: Int) = {
    val tokens = TextUtil.cleanText(text).split("\\s+")
    val d = getWordIds(tokens, word_dict, stopwords)
    val b = getBiterms(d).toArray
    b.map(x => Biterm(x, Random.nextInt(k))).toSeq
  }

  def createDictFromArray(words: Array[String]) = {
    Array.tabulate(words.length)(i => (words(i) -> i) ).toMap
  }

//  // ToDo: delete this function - change all references to use createWordDictFromWordcount below
//  def loadSavedWordDict(path: String, stopwords: Set[String]) = {
//    val words = Source.fromFile(path).getLines.map(_.split("\t")).map(x => x(0)).filter(w => !(stopwords contains w)).toArray
//    val dict: Map[String, Int] = Array.tabulate(words.length)(i => (words(i) -> i) ).toMap
//    (words, dict)
//  }

  def createWordDictFromWordcount(path: String, stopwords: Set[String], minCount: Int = 1) = {
    // val wordcounts = Source.fromFile(wcpath).getLines.map(_.split("\t")).map(x => (x(0), x(1))).toMap
    val word_counts = Source.fromFile(path).getLines.map(_.split("\t")).toArray.map(x => (x(0), x(1).toInt))
    val words = word_counts.filter(x => x._2 > minCount).sortWith(_._2 > _._2).map(_._1)
    val filtered = words.filter(w => !(stopwords contains w ))
    val word_dict = createDictFromArray(filtered)
    (filtered, word_dict)
  }

  // ===================  save model, topics ====================================================================

  def save_result(theta: Array[Double], phi: Array[Double], path: String, k: Int) = {
    info(s"Saving theta and phi to ${path}...")
    val outpath_pz = s"${path}_pz_theta.txt"
    val outpath_pwz = s"${path}_pw|z_phi.txt"
    val out_pz = new PrintWriter(new File(outpath_pz))
    out_pz.println ( theta.mkString("\n") )
    val out_pwz = new PrintWriter(new File(outpath_pwz))
    for (p <- phi) {
      out_pwz.println(p)
    }
    out_pz.close
    out_pwz.close
  }

  def save_obtm_priors(alpha_prior: Array[Double], beta_prior: Array[Double], path: String) = {
    val patha = s"${path}_alpha_prior.txt"
    val pathb = s"${path}_beta_prior.txt"
    val outa = new PrintWriter(new File(patha))
    val outb = new PrintWriter(new File(pathb))
    alpha_prior.foreach{ a => outa.println(a.toString) }
    beta_prior.foreach{ b => outb.println(b.toString) }
    outa.close
    outb.close
  }

  def load_obtm_priors(alpha_path: String, beta_path: String) = {
    val alpha_prior = Source.fromFile(alpha_path).getLines.map(_.toDouble).toArray
    val beta_prior = Source.fromFile(beta_path).getLines.map(_.toDouble).toArray
    (alpha_prior, beta_prior)
  }


  def loadTheta(path: String) = {
    val theta = Source.fromFile(path).getLines.map(x => x.toDouble).toArray
    theta
  }

  def loadPhi(path: String) = {
    val phi = Source.fromFile(path).getLines.map(x => x.toDouble).toArray
    phi
  }

  def save_topics(topic_dist: Array[(Double, scala.collection.immutable.Seq[String])], path: String, k: Int) = {
    info(s"Saving k=${k} topics to ${path}...")
    // val out_topic = s"${path}_topics.${k}.k.txt"
    val out_topic = s"${path}_topics.txt"
    val out = new PrintWriter(new File(out_topic))
    topic_dist.map(td => td._1 + "\t" +  td._2.mkString(", ")) foreach { out.println }
    out.close
  }

  def save_topics_counts(topic_dist: Array[(Double, scala.collection.immutable.Seq[String])], nz: Array[Long], date: String, iterN: Int, m: Int, duration: String, path: String) = {
    val k = topic_dist.size
    info(s"Saving k=${k} topics to ${path}...")
    val out_topic = s"${path}_topicCounts.txt"
    val out = new PrintWriter(new File(out_topic))
    out.println(s"# Date: ${date}\titerN: ${iterN}\tM: ${m}\tRunning time: ${duration}")
    out.println("#"+ "-" * 80)
    out.println("#Z\tCount\tp(z)\t\t\tTop 20 terms descending")
    out.println("#"+ "-" * 80)
    topic_dist.zipWithIndex.map{case (td, i) => i  + "\t" + nz(i) + "\t" + td._1 + "\t" +  td._2.mkString(", ") } foreach { out.println }
    out.close
  }

  def save_biterms(biterms: Array[Biterm], path: String, k: Int) = {
    // val outpath_bi = s"${path}_biterms.${k}.k.txt"
    val outpath_bi = s"${path}_biterms.txt"
    val data = biterms.map {b =>
      val (w1, w2) = b.biterm
      val z = b.z
      w1 + "\t" + w2 + "\t" + z
    }
    val out_bi = new PrintWriter(new File(outpath_bi))
    data foreach { out_bi.println }
    out_bi.close
  }

  def loadSavedBiterms(path: String) = {
    val biterms = Source.fromFile(path).getLines.map(_.split("\t")).map(x => Biterm((x(0).toInt, x(1).toInt), x(2).toInt) ).toArray
    biterms
  }

  def fillUpSlidingWindow(biterms: Array[Biterm], bs_win: ArrayBuffer[Biterm], window: Int = 2000000) = {
    var i = 0
    for (bi <- biterms) {
      if (bs_win.size < window) bs_win += bi
      else { bs_win(i % window) = bi
        i += 1 }
    }
  }

  // ===================  reports ====================================================================

  def run_reports(theta: Array[Double], phi:Array[Double], words: Array[String], m: Int, k: Int) = {
    val top_dist = report_topics(theta, phi, words, m, k)
    for (td <- top_dist) println(td._1 + "\t" +  td._2.mkString(", "))
    // val (word, p_z_w) = report_words(phi, words, theta, m, k)
  }

  def report_topics(theta: Array[Double], phi:Array[Double], words: Array[String], m: Int, k: Int, numWords: Int = 20) = {
    def top_indices(m: Int, z: Int, k: Int) = {
      val ws = (0 until m).sortBy(w => -phi(w*k+z)).take(numWords)
      ws
    }
    def get_words(ws: Array[Int], words: Array[String]) = {
      val top_words = ws.map(words).toSeq
      top_words
    }
    val topic_distribution = Iterator.range(0, k).toArray.map { z =>
      val ws = (0 until m).sortBy(w => -phi(w*k+z)).take(20)
      (theta(z), ws.map(words).asInstanceOf[Seq[String]]) // XXX was .toSeq
    }
    topic_distribution
  }

//  XXX Remove?
  def printTopicDist(topic_dist: Array[(Double, scala.collection.immutable.Seq[String])]) = {
    println("\n\n")
    for (td <- topic_dist) println(td._1 + "\t" +  td._2.mkString(", "))
    println("\n\n")
  }

  def report_words(phi:Array[Double], words: Array[String], theta: Array[Double], m: Int, k: Int) = {
    words.zipWithIndex.map { wordIndex =>
      val (word, w) = wordIndex
      val p_w_z = Iterator.range(w*k, (w+1)*k).map(phi)
      val weight = p_w_z.zip(theta.iterator).map { case (p, q) => p * q }.toArray
      val h = 1.0 / weight.sum
      val p_z_w = weight.map(_ * h)
      (word, p_z_w)
    }
  }

  def report_documents(documents:Array[(String, Array[Int])], theta: Array[Double],  phi:Array[Double], k: Int) = {
    val doc_k_distribution = documents.map { case (id, d) =>
      val bs = getBiterms(d).toArray
      val hs = bs.map { b =>
        val (w1, w2) = b
        1.0 / Iterator.range(0, k).map { z =>
          theta(z) * phi(w1*k+z) * phi(w2*k+z)
        }.sum * bs.count(_ == b) / bs.length
      }

      val p_z_d = Iterator.range(0, k).toArray.map{ z =>
        bs.zip(hs).map { case (b, h) =>
          val (w1, w2) = b
          theta(z) * phi(w1*k+z) * phi(w2*k+z) * h
        }.sum
      }
      (id, p_z_d)
    }
    doc_k_distribution
  }

  // same as above but input text (not word_ids) and also return input text
  def predict_documents(documents: Array[(String, String)], theta: Array[Double],  phi:Array[Double], k: Int, word_dict: Map[String, Int], stopwords: Set[String]) = {
    def t2wids(text: String) = {
      val tokens = TextUtil.cleanText(text).split("\\s+")
      val d = getWordIds(tokens, word_dict, stopwords)
      d
    }
    val doc_k_distribution = documents.map { case (id, d) =>
      val ts = t2wids(d)
      val bs = getBiterms(ts).toArray
      val hs = bs.map { b =>
        val (w1, w2) = b
        1.0 / Iterator.range(0, k).map { z =>
          theta(z) * phi(w1*k+z) * phi(w2*k+z)
        }.sum * bs.count(_ == b) / bs.length
      }

      val p_z_d = Iterator.range(0, k).toArray.map{ z =>
        bs.zip(hs).map { case (b, h) =>
          val (w1, w2) = b
          theta(z) * phi(w1*k+z) * phi(w2*k+z) * h
        }.sum
      }
      (id, d, p_z_d)
    }
    doc_k_distribution
  }


  /**
   * Input: Array of documents (doc_id, doc_text)
   * Return: Array of (doc_id, doc_text, p_z_d distribution)
   * Return: Array of (k, (doc_id, doc_text, predicted_label, confidence_score))
   */
  def top20_docs(documents: Array[(String, String)], theta: Array[Double], phi: Array[Double], k: Int, word_dict: Map[String, Int], stopwords: Set[String]) = {

    val doc_dist = predict_documents(documents, theta, phi, k, word_dict, stopwords)

    val t_dist = Iterator.range(0, k).toArray.map { z =>
      val doc_dist_fil = doc_dist.filter(x => indexOfLargest(x._3) == z)
      val ds = doc_dist_fil.sortBy(x => -x._3(z)).take(200).toList
      var textset = Set[String]()
      val top20 = ds.filterNot { row =>
        val (tid, text, dist) = row
        val bool = textset(TextUtil.cleanText(row._2))
        textset += TextUtil.cleanText(row._2)
        bool }.take(20)

      (z, top20.toSeq)
    }
    t_dist
  }

  /*
      Receive:  a text string
      Return: a predicted topic label and confidence score: (k, probability)
   */
  def predict_single_document(text: String,  theta: Array[Double], phi: Array[Double], k: Int, word_dict: Map[String, Int], stopwords: Set[String]) = {
    val tokens = TextUtil.cleanText(text).split("\\s+")
    val d = getWordIds(tokens, word_dict, stopwords)
    val bs = getBiterms(d).toArray

    val hs = bs.map { b =>
      val (w1, w2) = b
      1.0 / Iterator.range(0, k).map { z =>
        theta(z) * phi(w1*k+z) * phi(w2*k+z)
      }.sum * bs.count(_ == b) / bs.length
    }
    val p_z_d = Iterator.range(0, k).toArray.map{ z =>
      bs.zip(hs).map { case (b, h) =>
        val (w1, w2) = b
        theta(z) * phi(w1*k+z) * phi(w2*k+z) * h
      }.sum
    }
    val idx = indexOfLargest(p_z_d)
    (idx, p_z_d(idx))
  }

  def auto_label2(topic_dist: Array[(Double, Seq[String])] ): Array[(Double, Seq[String], Seq[String])] = {
    // extract top 3 hashtags from each topic, append these 'labels' to each row
    def find_labels(tp: Seq[String]): Seq[String] = {
      val hashtags = tp.filter(_.startsWith("#")).take(3)
      val terms = tp.filterNot(_.startsWith("#")).take(3)
      val labels = if (hashtags.size >= 3) hashtags else  hashtags ++ terms take(3)
      labels
    }
    val labeled = topic_dist.map{ case (theta, tpcs) => (theta, find_labels(tpcs), tpcs ) }
    labeled
  }


  def print_topics(labeled_topic_dist: Array[(Double, scala.collection.immutable.Seq[String], scala.collection.immutable.Seq[String])], nzMap: scala.collection.immutable.Map[Int,Long], date: String, iterN: Int, m: Int, alpha: Double, beta: Double, duration: String) = {
    val k = labeled_topic_dist.size
    println(s"# Date: ${date}\talpha: ${alpha}\tbeta: ${beta}\titerN: ${iterN}\tM: ${m}\tK: ${k}")
    println(s"# Running time:\t${duration}")
    println("#"+ "-" * 80)
    println("#Z\tCount\tp(z)\t\t\tTop terms descending")
    println("#"+ "-" * 80)
    labeled_topic_dist.zipWithIndex.map{case (td, i) => i  + "\t" + nzMap(i) + "\t" + td._1 + "\t" +  td._2.mkString(", ") + "\t->\t" + td._3.take(20).mkString(", ") } foreach { println }
  }

  // ===================  other ====================================================================

  def indexOfLargest(array: Array[Double]): Int = {
    val result = array.foldLeft(-1,Double.MinValue,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue > maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }

  def endsWithSep(path: String) = {
    path(path.length-1) == File.separatorChar
  }

  def pathify(path: String) = path match {
    case x if endsWithSep(x) => path
    case _ => path + File.separatorChar
  }

  // ====================  Measure Running Time  ======================

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("\n\tRunning time: " + (t1 - t0) / 1000000000  + " sec\n")
    result
  }

  // ===================  date handling ====================================================================
  import java.text.SimpleDateFormat
  import java.util.Date
  import java.util.TimeZone

  def getDateObj(tstamp: String) = {
    val tz = TimeZone.getTimeZone("GMT")
    val inFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    inFmt.setTimeZone(tz)
    val parsed = inFmt.parse(tstamp)
    parsed
  }

  /*  Convert a Twitter 'created_at' timestamp into year-month-date format
      Receive: Fri Jan 02 19:05:00 +0000 2015
      Return: String = 2015-01-02
   */
  def ca2ymd(tstamp: String) = {
    val parsed = getDateObj(tstamp)
    val outFmt = new SimpleDateFormat("yyyy-MM-dd")
    val tz = TimeZone.getTimeZone("GMT")
    outFmt.setTimeZone(tz)
    val ymd = outFmt.format(parsed)
    ymd
  }

  /*
      Example: Receive:    Fri Jan 02 19:05:00 +0000 2015
               Return:     java.util.Date = Fri Jan 02 00:00:00 EST 2015
   */
  def ca2ymdDate(tstamp: String) = {
    val outFmt = new SimpleDateFormat("yyyy-MM-dd")
    val adate = outFmt.format(getDateObj(tstamp))
    outFmt.parse(adate)
  }

}


