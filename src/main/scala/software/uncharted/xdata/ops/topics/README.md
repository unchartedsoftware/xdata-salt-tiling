# Twitter Topic Modelling
This operation is an implementation of the xdata [topic modelling](https://stash.uncharted.software/projects/XDATA/repos/topic-modelling/browse) project. Below are the notes associated with that project (which may be out of date since porting to pipeline op):
## Original Research Notes:

This project represents just the latest in a series of variations and extensions of Biterm Topic Models (Yan et al. 2013)
Variations implemented here include:
  - A Dirichlet Process for nonparametric Bayesian inference so the model can grow in complexity
  - Option of using weights (TFIDF scores) on words rather than +/- 1 for sample recorder counts
  - The word dictionary can be inferred for each day separately from the input text.

 General inputs:
    rdd                             - key-value RDD partitoned by date (date, (tweet_id, tweet_text))

BDPParallel input:
    iterator: Iterator[(String, (String, String))]      - date-partitioned records (date, (tweet_id, tweet_text))
    stpbroad: Set[String]]          - set of stopwords
    iterN: Int                      - number of iterations (150)
    k:Int                           - number of k topics to start with (2)
    alpha: Double                   - Dirichelet clumpiness hyperparameter (1/e)
    eta: Double                     - Dirichelet     hyperparameter (0.01)

Optional inputs:
    word_dict/ words                - dictionary

BDP input:
    biterms: Array[Biterm]
    words: Array[String]
    iterN: Int, k: Int, alpha: Double, eta: Double


#### (SEMI) FIXED PARAMETERS
val alpha = 1 / Math.E
val eta = 0.01
var k = 2


#### INPUT PARAMETERS
iterN:	the number of iterations of MCMC sampling to run
	- the given/default (150) is fine (empirically derived)

An array of dates
	- used to partition data by date & run each on a separate core
	- alternately list of dates could be inferred from the data

Path to Twitter data on HDFS
	- data must be preprocessed in two steps:
		(1) map to schema (date, id, text)
		(2) clean & normalize text


#### OTHER PARAMETERS
val lang = "en"
	- this actually isn't used in the example script
	- this allows you to possibly filter the input data by language

Stopwords
	- these are pretty much hard-coded. (stopwords don't change)
	- I have included stopword in resources. I will add some more to the repo.
	- if run globally (multilingual) concatenating all stopword files is fine

TFIDF scores
	- don't worry about this now.
	- A separate stage would have to compute TFIDF scores for a given corpus


#### OUTPUTTING RESULTS
topic_dist
	For annotating data you need to output the top N terms from topic_dist for
	each topic cluster.

(I write the results to a _local_ file for my own convenience.)


#### N.B.
theta:	global topic distribution
phi:	topic-word distribution
	- these are the parameters learned by MCMC inference.
	- It can be a good idea to keep them around (but not needed for annotation)
Note that once you have learned theta/phi you can subsequently use them to quickly
re-compute topic_dist, and/or quickly compute topic labels for a document
in the original set.

nzMap:	the number of documents in each cluster
	- probably not useful for annotation
