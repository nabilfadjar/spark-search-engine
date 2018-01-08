import org.apache.spark._

object QueryEngine {
    def main(args: Array[String]) {
        // Init App
        val conf = new SparkConf().setAppName("Spark Search Engine: Query Engine")
        val sc = new SparkContext(conf)

        if (args.length != 2) {
            System.err.println("Usage: QueryEngine [--main|--sample] <query>")
            sc.stop()
        }

        // Location of Sequence Files
        val index_loc_list = Map("main" -> "spark-search-engine/index", "sample" -> "spark-search-engine/sample_index")
        var index_loc = index_loc_list("sample")
        // sc.saveAsObjectFile(index_loc) // Save RDDs as Spark Objects (Sequence Files)
        // sc.objectFile(index_loc + "/") // Load Spark Objects (Sequence Files) as RDDs

        val query_string = args(1)
        if(args(0) == "--main"){
            index_loc = index_loc_list("main")
        }
        else if(args(0) == "--sample"){
            index_loc = index_loc_list("sample")
        }
        else {
            System.err.println("Usage: QueryEngine [--main|--sample] <query>")
            sc.stop()
        }

        //
        // Cosine Similarity
        //

        // Load IDF, TF_IDF Sets
        val idf_set = sc.objectFile[(String, Double)](index_loc + "/idf")
        val tf_idf_set_opt_list = sc.objectFile[(String, (Int, Double))](index_loc + "/tf_idf")

        //Get TF-IDF for Query
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
        // Formula = (query_tf_idf * post_tf_idf) / ( Math.sqrt(pow(query_tf_idf,2.0)) * Math.sqrt(pow(post_tf_idf,2.0)) )
        val posts_filter_cos = posts_filter_tf_idf_set.map(each_tf_idf_term_doc => (each_tf_idf_term_doc._2._2._1, ((each_tf_idf_term_doc._2._1 * each_tf_idf_term_doc._2._2._2), Math.pow(each_tf_idf_term_doc._2._2._2,2.0)) )).reduceByKey((a,b) => ((a._1+b._1), (a._2+b._2))).map(doc => (doc._1,(doc._2._1/(query_ecd_distance.value * Math.sqrt(doc._2._2)))))

        val posts_filter_cos_sort = posts_filter_cos.map(row => (row._2, row)).sortByKey(false).map(row => (row._2))
        posts_filter_cos.foreach(println)
        sc.stop()
        // val posts_filter_cos = posts_filter_tf_idf_set.map(eachTfIdf => (eachTfIdf._2._1,( (query_tf_idf * eachTfIdf._2._2) / ( Math.sqrt(Math.pow(query_tf_idf,2.0)) * Math.sqrt(Math.pow(eachTfIdf._2._2,2.0)) ) )) )
    }
}
