import Post

import org.apache.spark._

object GenerateTfIdf extends App {
    // Init App
    val conf = new SparkConf().setAppName("Spark Search Engine: Generate TF-IDF")
    val sc = new SparkContext(conf)

    // Location of Data
    val sample_data_loc = "spark-search-engine/sample_data/Posts.xml"
    // val main_data_loc = "/data/stackOverflow2017/Posts.xml"
    val data_loc = sample_data_loc

    // Location of Sequence Files
    val sample_index_loc = "spark-search-engine/sample_index"
    // val main_index_loc = "spark-search-engine/index"
    val index_loc = sample_index_loc
    // sc.saveAsObjectFile(index_loc) // Save RDDs as Spark Objects (Sequence Files)
    // sc.objectFile(index_loc + "/") // Load Spark Objects (Sequence Files) as RDDs

    // Parse XML posts as Post Objects
    var posts = sc.textFile(data_loc).map(row => new Post(row)).filter(_.getMap() != null)
    val posts_count = sc.broadcast(posts.count().toDouble)

    // Create Word Tuple for Word Count
    var wordTuple = posts.flatMap(_.getWordsFromBody().distinct).map(word => (word,1)).reduceByKey((a,b) => (a+b))

    // Generate TF Set
    var tf_set = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => ((word, eachPost.getId), 1.0/eachPost.getNumberOfWordsInPost))).reduceByKey((a,b) => (a+b))
    // var wordInPostTuple = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => (word, eachPost.getId) )).map(wordInPostKey => (wordInPostKey,1)).reduceByKey((a,b) => (a+b))

    var tf_set_preJoin = tf_set.map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))

    // Generate IDF Set
    var idf_set = wordTuple.map(eachWordTuple => (eachWordTuple._1,((Math.log(posts_count.value) - Math.log(eachWordTuple._2))/Math.log(Math.E))+1))

    // Generate TF-IDF Set
    // Method 1: Resulting Dataset is (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), ..., (word, (ID,TF-IDF))
    var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))
    var tf_idf_set_opt_list = tf_idf_set.map(row => (row._2._2, row)).sortByKey(false).map(row => (row._2)).sortByKey()

    // Method 2 (Not Being Used): Resulting Dataset is (word, CompactBuffer((ID,TF-IDF), (ID,TF-IDF), (ID,TF-IDF), ..., (ID,TF-IDF)))
    // var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))
    // var tf_idf_set_opt_cb= tf_idf_set.combineByKey()

    // Save TF-IDF set into HDFS
    idf_set.saveAsObjectFile(index_loc + "/idf") // Save IDF Set
    tf_idf_set_opt_list.saveAsObjectFile(index_loc + "/tf_idf") // Save TF_IDF Set

    // Stop Spark
    sc.stop()
}
