
# (SEMI) FIXED PARAMETERS
-------------------------
val alpha = 1 / Math.E
val eta = 0.01
var k = 2


# INPUT PARAMETERS
-------------------
iterN:	the number of iterations of MCMC sampling to run
	- the given/default (150) is fine (empirically derived)

An array of dates
	- used to partition data by date & run each on a separate core
	- alternately list of dates could be inferred from the data

Path to Twitter data on HDFS
	- data must be preprocessed in two steps:
		(1) map to schema (date, id, text
		(2) clean & normalize text


# OTHER PARAMETERS
-------------------
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


# OUTPUTTING RESULTS
--------------------
topic_dist
	For annotating data you need to output the top N terms from topic_dist for
	each topic cluster. 

(I write the results to a _local_ file for my own convenience.)


# n.b.
------
theta:	global topic distribution
phi:	topic-word distribution
	- these are the parameters learned by MCMC inference. 
	- It can be a good idea to keep them around (but not needed for annotation)
Note that once you have learned theta/phi you can subsequently use them to quickly 
re-compute topic_dist, and/or quickly compute topic labels for a document
in the original set. 

nzMap:	the number of documents in each cluster 
	- probably not useful for annotation
