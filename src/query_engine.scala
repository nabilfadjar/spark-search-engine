object query_engine extends App {
    // Location of Sequence Files
    val sample_index_loc = "spark-search-engine/sample_index"
    // val main_index_loc = "spark-search-engine/index"
    val index_loc = sample_index_loc
    // sc.saveAsObjectFile(index_loc) // Save RDDs as Spark Objects (Sequence Files)
    // sc.objectFile(index_loc + "/") // Load Spark Objects (Sequence Files) as RDDs

    //
    // Cosine Similarity
    //

    // Load IDF, TF_IDF Sets
    val idf_set = sc.objectFile[(String, Double)]("spark-search-engine/sample_index/idf")
    val tf_idf_set_opt_list = sc.objectFile[(String, (Int, Double))]("spark-search-engine/sample_index/tf_idf")

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
