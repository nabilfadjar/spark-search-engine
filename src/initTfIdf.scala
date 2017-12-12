object initTfIdf extends App {
    // Location of Data
    val sample_data_loc = "spark-search-engine/sample_data/Posts.xml"
    // val main_data_loc = "/data/stackOverflow2017/Posts.xml"

    // Location of Sequence Files
    val sample_index_loc = "spark-search-engine/sample_index"
    // val main_index_loc = "spark-search-engine/index"
    // sc.saveAsObjectFile(sample_index_loc) // Save RDDs as Spark Objects (Sequence Files)
    // sc.objectFile(sample_index_loc + "/") // Load Spark Objects (Sequence Files) as RDDs

    // Parse XML posts as Post Objects
    var posts = sc.textFile(sample_data_loc).map(row => new Post(row)).filter(_.getMap() != null)
    val posts_count = sc.broadcast(posts.count().toDouble)

    // Create Word Tuple for Word Count
    var wordTuple = posts.flatMap(_.getW ordsFromBody().distinct).map(word => (word,1)).reduceByKey((a,b) => (a+b))

    // Generate TF Set
    var tf_set = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => ((word, eachPost.getId), 1.0/eachPost.getNumberOfWordsInPost))).reduceByKey((a,b) => (a+b))
    // var wordInPostTuple = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => (word, eachPost.getId) )).map(wordInPostKey => (wordInPostKey,1)).reduceByKey((a,b) => (a+b))

    var tf_set_preJoin = tf_set.map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))

    // Generate IDF Set
    var idf_set = wordTuple.map(eachWordTuple => (eachWordTuple._1,(Math.log(posts_count.value) - Math.log(eachWordTuple._2))/Math.log(Math.E)))

    // Generate TF-IDF Set
    // Method 1: Resulting Dataset is (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), ..., (word, (ID,TF-IDF))
    var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))
    var tf_idf_set_opt_list = tf_idf_set.map(row => (row._2._2, row)).sortByKey(false).map(row => (row._2)).sortByKey()

    // Method 2: Resulting Dataset is (word, CompactBuffer((ID,TF-IDF), (ID,TF-IDF), (ID,TF-IDF), ..., (ID,TF-IDF)))
    // var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))
    // var tf_idf_set_opt_cb= tf_idf_set.combineByKey()

    //
    // Cosine Similarity
    //

    //Get TF-IDF for Query
    val query_string = "java code"
    val query = sc.parallelize(query_string.split(" "))
    val query_size = sc.broadcast(query.count().toDouble)
    val query_tf = query.map(query_term => (query_term, 1.0/query_size.value)).reduceByKey((a,b) => (a+b))
    val query_idf = query.map(query_term => (query_term, 1)).join(idf_set)
    val query_tf_idf = query_tf * query_idf

    //Get Post TF-IDF
    //val posts_idf = query_idf
    val posts_filter_tf_idf_set = tf_idf_set_opt_list.filter(_._1 == query)
    // formula = (query_tf_idf * post_tf_idf) / ( Math.sqrt(pow(query_tf_idf,2.0)) + Math.sqrt(pow(post_tf_idf,2.0)) )
    val posts_filter_cos = posts_filter_tf_idf_set.map(eachTfIdf => (eachTfIdf._2._1,( (query_tf_idf * eachTfIdf._2._2) / ( Math.sqrt(Math.pow(query_tf_idf,2.0)) * Math.sqrt(Math.pow(eachTfIdf._2._2,2.0)) ) )) )
}
