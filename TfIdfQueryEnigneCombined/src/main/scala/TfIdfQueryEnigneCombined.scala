import org.apache.spark._
import scala.collection.immutable.HashSet
import org.apache.spark.storage.StorageLevel._

/*
 * Post Class
 */
class Post(var toBeParsed: String) {
    private val postMap = if(isHeaderOrFooter()) null else transformIntoMap()
    toBeParsed = null;
    private val wordsInBody = extractWordsFromBody()

    private def transformIntoMap() : Map[String, String] = {
        return toBeParsed.split("(=\")|(\"[\\s])|(<[\\w]*)|(/>)").map(_.trim).filter(_.nonEmpty).grouped(2).collect { case Array(k, v) => k -> v }.toMap.filterKeys(Set("Id","Body"))
    }

    private def isHeaderOrFooter() : Boolean = {
        return (toBeParsed.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>") || toBeParsed.endsWith("posts>"))
    }

    def getMap() : Map[String,String] = {
        return postMap
    }

    def getId() : Int = {
        if (getMap() == null) return -1 else return postMap.get("Id").getOrElse("-1").toInt
    }

    def getBody() : String = {
        if (getMap() == null) return null else return postMap.get("Body").getOrElse(null)
    }

    private def extractWordsFromBody() : Array[String] = {
        if (getBody() == null) return null else return getBody().toLowerCase.replaceAll("&lt;code&gt;", "").replaceAll("(&[\\S]*;)|(&lt;[\\S]*&gt;)", " ").replaceAll("[\\s](a href)|(rel)[\\s]", " ").replaceAll("(?!([\\w]*'[\\w]))([\\W_\\s\\d])+"," ").split(" ").filter(_.nonEmpty)
    }

    def getWordsFromBody() : Array[String] = {
        if (wordsInBody == null) return null else return wordsInBody
    }

    def getNumberOfWordsInPost() : Int = {
        return getWordsFromBody().length
    }
}

/*
 * Main Class
 * TfIdfQueryEnigneCombined
 */
object TfIdfQueryEnigneCombined {
    def main(args: Array[String]) {
        /*
         * Initialise Spark Job
         */

         // Init SparkContext
        val conf = new SparkConf().setAppName("Spark Search Engine: Generate TF-IDF and Query Data")
        val sc = new SparkContext(conf)

        // Parse given arguements: Length Check
        if (args.length != 2) {
            System.err.println("Usage: TfIdfQueryEnigneCombined [--main|--sample] <query>")
            sc.stop()
        }

        // Location of Data
        val data_loc_list = Map("main" -> "/data/stackOverflow2017/Posts.xml", "sample" -> "spark-search-engine/sample_data/Posts.xml")
        var data_loc = data_loc_list("sample")

        // Location of Reults
        val result_loc_list = Map("main" -> "spark-search-engine/results", "sample" -> "spark-search-engine/sample_results")
        var result_loc = result_loc_list("sample")

        // Parse given arguements: Arguement Check
        val query_string = args(1)
        if(args(0) == "--main"){
            data_loc = data_loc_list("main")
            result_loc = result_loc_list("main")
        }
        else if(args(0) == "--sample"){
            data_loc = data_loc_list("sample")
            result_loc = result_loc_list("sample")
        }
        else {
            System.err.println("Usage: GenerateTfIdf [--main|--sample] <query>")
        }

        /*
         * Query Parsing and Cleansing
         * For this stage, we would parse the entire dataset and would filter out unnecessary data.
         */
        // Parse given query
        val filtered_query = query_string.toLowerCase.replaceAll("&lt;code&gt;", "").replaceAll("(&[\\S]*;)|(&lt;[\\S]*&gt;)", " ").replaceAll("[\\s](a href)|(rel)[\\s]", " ").replaceAll("(?!([\\w]*'[\\w]))([\\W_\\s\\d])+"," ").split(" ").filter(_.nonEmpty)
        val query = sc.parallelize(filtered_query)
        val query_size = sc.broadcast(query.count().toDouble)
        val query_asHashSet = sc.broadcast(new HashSet() ++ filtered_query)

        /*
         * Data Parsing and Cleansing
         * For this stage, we would parse the entire dataset and would filter out unnecessary data.
         */
        // Parse XML posts as Post Objects
        var posts = sc.textFile(data_loc).map(row => new Post(row)).filter(_.getMap() != null).persist(MEMORY_AND_DISK)
        val posts_count = sc.broadcast(posts.count().toDouble)
        var posts_query_filtered = posts.filter(eachPost => !(eachPost.getWordsFromBody().filter(word => query_asHashSet.value.contains(word)).isEmpty) ).persist(MEMORY_AND_DISK)

        // Create Word Tuple for Word Count and filter for query
        var wordTuple = posts_query_filtered.flatMap(_.getWordsFromBody().filter(word => query_asHashSet.value.contains(word)).distinct).map(word => (word,1)).reduceByKey((a,b) => (a+b))

        /*
         * Generate TF Set
         * Creates a (Word, ID) tuple and assigns it a weight shared by all terms in the same post (1.0/eachpost…), all stored in an RDD.
         * (Word, ID) tuple is filtered to only words given by the query.
         */
        var tf_set = posts_query_filtered.flatMap(eachPost => eachPost.getWordsFromBody().filter(word => query_asHashSet.value.contains(word)).map(word => ((word, eachPost.getId), 1.0/eachPost.getNumberOfWordsInPost))).reduceByKey((a,b) => (a+b))
        var tf_set_preJoin = tf_set.map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))

        // Generate IDF Set
        var idf_set = wordTuple.map(eachWordTuple => (eachWordTuple._1,((Math.log(posts_count.value) - Math.log(eachWordTuple._2))/Math.log(Math.E))+1)).persist(MEMORY_AND_DISK)

        // Generate TF-IDF Set
        // Method 1: Resulting Dataset is (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), (word, (ID,TF-IDF)), ..., (word, (ID,TF-IDF))
        var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))

        if(query_size.value == 1){
            val tf_idf_set_sort = tf_idf_set.map(row => (row._2._2, row._2._1)).sortByKey(false).map(row => (row._2, row._1)).take(10)

            // Save Results in HDFS
            sc.parallelize(tf_idf_set_sort).saveAsTextFile(result_loc + "/search_results_id")

            val postID_asHashSet = sc.broadcast(new HashSet() ++ tf_idf_set_sort.map(row => (row._1.toInt)))
            var tf_idf_set_sort_xml = posts.filter(post => postID_asHashSet.value.contains(post.getId())).map(post => (post.getId(),post.getBody()))
            tf_idf_set_sort_xml.saveAsTextFile(result_loc + "/search_results_xml")
        }
        else {
            /*
             * Cosine Similarity
             * Part 1: Calculating TF-IDF for Query Set
             * For Cosine Similarity, we would need to calculate the TF-IDF for the given query first.
             * For now, we are able to calculate only TF first.
             */
            // Get TF-IDF for Query
            val query_tf = query.map(query_term => (query_term, 1.0/query_size.value)).reduceByKey((a,b) => (a+b))
            val query_tf_idf = query_tf.join(idf_set).map(word => (word._1, word._2._1 * word._2._2))

            // Euclidean distance for Query
            val query_ecd_distance = sc.broadcast(Math.sqrt(query_tf_idf.map(eachQuery => Math.pow(eachQuery._2, 2.0)).reduce(_ + _)))

            /*
             * Get Post TF-IDF
             * (word, (tf-idf_query, (doc_id, tf-idf_doc)))
             * (code,(0.8278593324990533,(199,0.007701017046502822)))
             * (java,(1.6069316415223283,(89,0.08240675084729888)))
             */
            val posts_filter_tf_idf_set = query_tf_idf.join(tf_idf_set)

            // Cosine Similarity Operation
            // Dot Product = (query_tf_idf_1 * post_tf_idf) + (query_tf_idf_2 * post_tf_idf) ... + (query_tf_idf_n * post_tf_idf)
            // Euclidean distance = Math.sqrt(Math.pow(query_tf_idf_1,2) + Math.pow(query_tf_idf_2,2) ... + + Math.pow(query_tf_idf_n,2))
            // Cosine Similarity = Dot Product / Cosine Similarity
            // Formula = (query_tf_idf * post_tf_idf) / ( Math.sqrt(pow(query_tf_idf,2.0)) * Math.sqrt(pow(post_tf_idf,2.0)) )
            val posts_filter_cos = posts_filter_tf_idf_set.map(each_tf_idf_term_doc => (each_tf_idf_term_doc._2._2._1, ((each_tf_idf_term_doc._2._1 * each_tf_idf_term_doc._2._2._2), Math.pow(each_tf_idf_term_doc._2._2._2,2.0)) )).reduceByKey((a,b) => ((a._1+b._1), (a._2+b._2))).map(doc => (doc._1,(doc._2._1/(query_ecd_distance.value * Math.sqrt(doc._2._2)))))
            val posts_filter_cos_sort = posts_filter_cos.map(row => (row._2, row)).sortByKey(false).map(row => (row._2)).take(10)

            // Save Results in HDFS
            sc.parallelize(posts_filter_cos_sort).saveAsTextFile(result_loc + "/search_results_id")

            val postID_asHashSet = sc.broadcast(new HashSet() ++ posts_filter_cos_sort.map(row => (row._1.toInt)))
            var posts_filter_cos_sort_xml = posts.filter(post => postID_asHashSet.value.contains(post.getId())).map(post => (post.getId(),post.getBody()))
            posts_filter_cos_sort_xml.saveAsTextFile(result_loc + "/search_results_xml")

        }

        // Stop Spark
        sc.stop()
    }
}
