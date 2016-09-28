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

package software.uncharted.xdata.ops.topics.twitter.util

import grizzled.slf4j.Logging
import org.apache.spark.broadcast.Broadcast


/**
  * NOTE:
  * This is essentially the same as the code in the standard BTM algorithm. The changes are:
  *     Sample Recorders
  *         the nonparametric Bayesian approach allows for unbounded values of K. This means
  *         that the sample recorders can expand (and contract!) to hold different numbers
  *         of 'topics'. As a first effort I have kept as much of the BTM code logic the same
  *         as possible but changed the sample recorders to using MutableArrays rather than Arrays
  *     Markov Chain Monte Carlo (MCMC) sampling
  *         n.b. we use an MCMC to estimate the conditional posterior for an unbounded K
  *         draw topic index z using CRP + stick breaking
  *         if z = k_new, increment the number of topics (tables), add to sample recorders
  *         if nz(z) == 0 (i.e. if there are no samples assigned to topic z) remove the topic (table), remove from sample recorders
  *         returns K, theta, phi (standard BTM returns theta, phi)
  **/
class BDP(kK: Int) extends Serializable with Logging { // TODO enable logging
  var k = kK
  var tfidf_dict: scala.collection.immutable.Map[Int,Double] = scala.collection.immutable.Map[Int,Double]()

  def initTfidf(tfidf_bcst: Broadcast[Array[(String, String, Double)]],
                date: String, word_dict: scala.collection.immutable.Map[String, Int]
               ) : Map[Int, Double] = {
    val tfidf = tfidf_bcst.value
    TFIDF.filterTfidf(tfidf, date, word_dict)
  }

  // ============================= Markov Chain Monte Carlo (MCMC)  ============================================
  def estimateMCMC(biterms:Array[Biterm], iterN: Int, model: SampleRecorder, m: Int, alpha: Double, eta: Double): (Int, Double) = {
    val start = System.nanoTime
    Iterator.range(0, iterN).foreach { iteration =>
      info(s"iteration: ${iteration + 1}\tk = $k")
      val bstart = System.nanoTime
      biterms.foreach { case b =>
        updateBiterm(b, model, m, alpha, eta)
      }
      // removeEmptyClusters & defrag
      k = model.defrag()
      val bend = System.nanoTime
      val runningtime = (bend - bstart) / 1E9
      info("\t time: %.3f sec.".format(runningtime))
    }
    val duration = (System.nanoTime - start) / 1E9 / 60.0
    info(s"Elapsed time: %.4f min".format(duration))
    (k, duration)
  }

  def updateBiterm(b: Biterm, model: SampleRecorder, m: Int, alpha: Double, eta: Double) : Unit = {
    unsetTopic(model, b)
    val z = drawTopicIndex(model, b, m, alpha, eta)

    // increment value of K if z > k
    if (z == k) {
      info("\t+1\t")
      k = model.addCluster()
    }
    if (z >= k) info(s"\n\n WHY IS Z > K ? \nz is $z and k is $k\n")
    if (z > k) info("\n\n********\n z should not be greater than k !!!\n\n")
    setTopic(model, b, z)
  }

  def setTopic(model: SampleRecorder, b:Biterm, z:Int) : Unit = {
    b.z = z
    model.increment(z, b.biterm)
  }

  def unsetTopic(model: SampleRecorder, b:Biterm) : Unit = {
    model.decrement(b.biterm, b.z, k)
  }


  // =============================   MCMC Sampling   =============================
  def drawTopicIndex(model: SampleRecorder, b: Biterm, m: Int, alpha: Double, eta: Double) : Int = {
    val dist = conditionalPosterior(model, b.biterm, m, alpha, eta)
    sample(dist)
  }

  // calculate PDF: the conditional posterior for all k (i.e. the likelihood of generating biterm b)
  def conditionalPosterior(model: SampleRecorder, b: (Int, Int), m: Int, alpha: Double, eta: Double) : Array[(Double, Int)] = {
    val (w1, w2) = b
    // calculate f(•), the density of Mult(z) created by CRP
    def density(z: Int) = {
      val numerator = (model.getNwz(z, w1) + eta) * (model.getNwz(z, w1) + eta)
      val denominator = Math.pow( 2 * model.getNz(z) + m * eta, 2)
      val d = numerator.toDouble / denominator.toDouble
      model.getNz(z) * d
    }
    def densityNew() = {
      val numerator = ( 0 + eta) * ( 0 + eta)
      val denominator = Math.pow(  0 + (m * eta), 2)
      alpha * (numerator / denominator)
    }
    val pd_existing = Iterator.range(0, k).map{ z => density(z) }.toArray
    val pd_new = densityNew()
    val dist = pd_existing ++ Array(pd_new)
    val total = dist.sum
    val norm_dist = dist.map(_ * ( 100 / total / 100))
    norm_dist.zipWithIndex.sortWith(_._1 > _._1) // associate each prob with its topic index, sort descending to make sampling more efficient
  }

  def sample[A](dist: Array[(Double, A)]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (itemProb, item) = it.next
      accum += itemProb
      if (accum >= p) {
        return item  // return so that we don't have to search through the whole distribution
      }
    }
    sys.error(f"this should never happen")  // needed so it will compile
  }

  // ============================= Estimate parameters  =============================
  def calcTheta(model: SampleRecorder, n_biterms: Int, k: Int, alpha: Double) : Array[Double] = {
    Iterator.range(0, k).map { z =>
      (model.getNz(z) + alpha) / (n_biterms + k * alpha)
    }.toArray
  }

  def calcPhi(model: SampleRecorder, m: Int, k: Int, beta: Double) : Array[Double] = {
    Iterator.range(0, m).flatMap { w =>
      Iterator.range(0, k).map { z =>
        (model.getNwz(z, w) + beta) / (model.getNz(z) * 2 + m * beta)
      }
    }.toArray
  }

  def estimateThetaPhi(model: SampleRecorder, n_biterms: Int, m: Int, k: Int, alpha: Double, beta: Double) : (Array[Double], Array[Double]) = {
    (calcTheta(model, n_biterms, k, alpha ), calcPhi(model, m, k, beta ))
  }

  // scalastyle:off magic.number
  def fit(biterms: Array[Biterm],
    words: Array[String],
    iterN: Int,
    k: Int,
    alpha: Double,
    eta: Double,
    weighted: Boolean = false,
    topT: Int = 100
  ) : (Array[(Double, Seq[Int])], Array[Double], Array[Double], Map[Int,Int], Double) = {
    val m = words.length
    val SR = new SampleRecorder(m, k, weighted)
    if (weighted) { SR.setTfidf(tfidf_dict) }
    SR.initRecorders(biterms)
    val btmDp = new BDP(k)
    info("Running MCMC sampling...")
    val (newK, duration) = estimateMCMC(biterms, iterN, SR, m, alpha, eta)
    val n_biterms = biterms.length
    info("Calculating phi, theta...")
    val (theta, phi) = estimateThetaPhi(SR, n_biterms, m, newK, alpha, eta )
    info("Calculating topic distribution...")
    // take top words for a topic (default is top 100 words)
    val topic_dist = BTMUtil.reportTopics(theta, phi, words, m, newK, topT)
    val nzMap = SR.getNzMap().toMap[Int, Int]
    (topic_dist, theta, phi, nzMap, duration)
  }
}
