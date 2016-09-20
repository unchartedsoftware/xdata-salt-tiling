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


// TODO:
// Have to remove ALL punctuation, symbols EXCEPT for {#_-} - used in hashtags
// Have to remove all {#_} which are NOT in hashtags? ... better hashtag detection?
// Have to confirm that this all white space is being replaced with a single white space (i.e. including non-English white space)
object TextUtil extends Serializable {
  private val mt = "@[\\w_]+\\b".r   // twitter user mentions
  private val url = "\\b(http[:\\.\\/\\w]+)\\b".r  // valid URLs
  private val nums = "\\b([0-9]+)\\b".r // digits
  private val apos = "'+".r   // apostrophe
  private val ps = "[\\p{S}\\p{Pd}\\p{Ps}\\p{Pe}\\p{Pi}\\p{Pf}\\p{Pc}!@%&*:;',.?/\\\"…]+".r   // punctuation
  private val ps2 = "[\\uFF1A-\\uFF20\\uFF3B-\\uFF3E\\uFF40\\uFF5B-\\uFF64\\\\]".r    // addtional punctuation
  private val srt = "\\b(\\w\\w?)\\b".r   // short words (1 or 2 characters only)
  private val single_hashtag = " ?#+ ".r  // valid hashtag (ToDo - use the regex from Twitter's GitHub for better hashtag detection
  private val nl = "[\n\r\t]+".r  // new line & tab characters
  private val unicodeOutliers = ("([^\\u0000-\\uFFEF]|[❤])").r  // unicode characters outside of the normal character bounds
  private val unicode_space = "[\\u0020\\u00A0\\u1680\\u180E\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200A\\u200B\\u202F\\u205F\\u3000\\uFEFF]+".r   // white space
  private val dots = "[\\u00B7\\u2024\\u2219\\u25D8\\u25E6\\u30FB\\uFF65]".r  // elipsis


  def cleanStopwords(text: String, stopwords: Set[String]) = {
    val cleaned = cleanText(text).split("\\s+").filter(w => !(stopwords contains w))
    cleaned.mkString(" ")
  }

  def cleanText(text: String) = {
    val no_emoji = unicodeOutliers.replaceAllIn(text, " ")
    val norm_hashtags = "[\\#]+".r.replaceAllIn(no_emoji, "#")
    val no_nl = nl.replaceAllIn(norm_hashtags, " ")
    val no_url = url.replaceAllIn(no_nl, "")
    val no_mt = mt.replaceAllIn(no_url, " ")
    val no_nums = nums.replaceAllIn(no_mt, " ")
    val no_apos = apos.replaceAllIn(no_nums, " ")
    val no_ps = ps.replaceAllIn(no_apos, " ")
    val no_ps2 = ps2.replaceAllIn(no_ps, " ")
    val no_dots = dots.replaceAllIn(no_ps2, " ")
    val no_short = srt.replaceAllIn(no_dots, " ")
    val no_hashtags = single_hashtag.replaceAllIn(no_short, " ").toLowerCase
    val respaced = unicodeOutliers.replaceAllIn(no_hashtags, " ")
    val cleaned = "[\\p{Zs}]+".r.replaceAllIn(respaced, " ").toLowerCase
    cleaned.trim
  }


  def prep(text: String) = {
    val cleaned = cleanText(text)
    cleaned.split("\\s+").toSeq
  }
}
