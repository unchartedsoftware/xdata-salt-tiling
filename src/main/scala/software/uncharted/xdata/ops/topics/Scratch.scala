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

object Scratch {
    def main(args: Array[String]) = {
//        val stream: InputStream = getClass.getResourceAsStream("/Stopwords/stopwords_html_tags.txt")
//        val lines = scala.io.Source.fromInputStream(stream).getLines
//        lines.foreach{ println }

        val swfiles = List("/Stopwords/stopwords_all_en.v2.txt",  "/Stopwords/stopwords_ar.txt",  "/Stopwords/stopwords_html_tags.txt")
        val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
        stopwords.foreach{ println }
    }
}
