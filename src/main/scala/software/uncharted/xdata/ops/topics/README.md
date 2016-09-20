# Biterm Topic Models

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

##   BUILD
$ mvn clean package

##   RUN
Set the number of cores and executor memory as you see fit. If running pseudo-parallel the
number of cores should be equal to the number of dates to process or a multiple thereof.

Note the example driver memory and kryroserizer buffer memory requirements - they are
larger than usual.

$ spark-shell --master yarn-client
                --executor-cores 4
                --num-executors 3
                --executor-memory 5G
                --driver-memory 3g
                --conf spark.kryoserializer.buffer=256
                --conf spark.kryoserializer.buffer.max=512
                --jars target/btm-1.0-SNAPSHOT.jar


Note: This algorithm takes a *long* time to run.
The running time is a function of:
    sample recorder size (vocabulary size, number of topics discovered)
    number of iterations of MCMC estimation
If run in the shell partitioned by date there will be no console output and it might
incorrectly appear to be hung
