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
    val idf_set = sc.objectFile[(String, Double)](sample_index_loc + "/idf")
    val tf_idf_set_opt_list = sc.objectFile[(String, (Int, Double))](sample_index_loc + "/tf_idf")

    //Get TF-IDF for Query
    val query_string = "&lt;p&gt;When is it not cool to make an appropriate to use an unsigned variable over a signed one? What about in a &lt;code&gt;for&lt;/code&gt; loop?&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I hear a lot of opinions about this and I wanted to see if there was anything resembling a consensus. &lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;for (unsigned int i = 0; i &amp;lt; someThing.length(); i++) {  &#xA;    SomeThing var = someThing.at(i);  &#xA;    // You get the idea.  &#xA;}&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;I know Java doesn't have unsigned values, and that must have been a concious decision on &lt;a href=&quot;https://en.wikipedia.org/wiki/Sun_Microsystems&quot; rel=&quot;noreferrer&quot;&gt;Sun Microsystems&lt;/a&gt;' part. &lt;/p&gt;&#xA;"
    val filtered_query = query_string.toLowerCase.replaceAll("&lt;code&gt;", "").replaceAll("(&[\\S]*;)|(&lt;[\\S]*&gt;)", " ").replaceAll("[\\s](a href)|(rel)[\\s]", " ").replaceAll("(?!([\\w]*'[\\w]))([\\W_\\s\\d])+"," ").split(" ").filter(_.nonEmpty)
    val query = sc.parallelize(filtered_query)
    val query_size = sc.broadcast(query.count().toDouble)
    val query_tf = query.map(query_term => (query_term, 1.0/query_size.value)).reduceByKey((a,b) => (a+b))
    val query_tf_idf = query_tf.join(idf_set).map(word => (word._1, word._2._1 * word._2._2))

    // Euclidean distance for Query
    val query_ecd_distance = sc.broadcast(Math.sqrt(query_tf_idf.map(eachQuery => Math.pow(eachQuery._2, 2.0)).reduce(_ + _)))

    //Get Post TF-IDF
    //val posts_idf = query_idf
    val posts_filter_tf_idf_set = query_tf_idf.join(tf_idf_set_opt_list)

    // Sample
    // (code,(0.8278593324990533,(199,0.007701017046502822)))
    // (java,(1.6069316415223283,(89,0.08240675084729888)))
    // Dot Product = (query_tf_idf_1 * post_tf_idf) + (query_tf_idf_2 * post_tf_idf) ... + (query_tf_idf_n * post_tf_idf)
    // Euclidean distance = Math.sqrt(Math.pow(query_tf_idf_1,2) + Math.pow(query_tf_idf_2,2) ... + + Math.pow(query_tf_idf_n,2))
    // Cosine Similarity = Dot Product / Cosine Similarity
    // Formula = (query_tf_idf * post_tf_idf) / ( Math.sqrt(pow(query_tf_idf,2.0)) + Math.sqrt(pow(post_tf_idf,2.0)) )
    val posts_filter_cos = posts_filter_tf_idf_set.map(each_tf_idf_term_doc => (each_tf_idf_term_doc._2._2._1, ((each_tf_idf_term_doc._2._1 * each_tf_idf_term_doc._2._2._2), Math.pow(each_tf_idf_term_doc._2._2._2,2.0)) )).reduceByKey((a,b) => ((a._1+b._1), (a._2+b._2))).map(doc => (doc._1,(doc._2._1/(query_ecd_distance.value * Math.sqrt(doc._2._2)))))

    val posts_filter_cos_sort = posts_filter_cos.map(row => (row._2, row)).sortByKey(false).map(row => (row._2))
    posts_filter_cos.foreach(println)
    // val posts_filter_cos = posts_filter_tf_idf_set.map(eachTfIdf => (eachTfIdf._2._1,( (query_tf_idf * eachTfIdf._2._2) / ( Math.sqrt(Math.pow(query_tf_idf,2.0)) * Math.sqrt(Math.pow(eachTfIdf._2._2,2.0)) ) )) )
}
