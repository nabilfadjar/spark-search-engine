import org.apache.spark._

class Post(val toBeParsed: String) {
    private val postMap = if(isHeaderOrFooter()) null else transformIntoMap()
    private val wordsInBody = extractWordsFromBody()

    private def transformIntoMap() : Map[String, String] = {
        return toBeParsed.split("(=\")|(\"[\\s])|(<[\\w]*)|(/>)").map(_.trim).filter(_.nonEmpty).grouped(2).collect { case Array(k, v) => k -> v }.toMap
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

object GenerateTfIdf {
    def main(args: Array[String]) {
        // Init App
        val conf = new SparkConf().setAppName("Spark Search Engine: Generate TF-IDF")
        val sc = new SparkContext(conf)

        if (args.length != 1) {
            System.err.println("Usage: GenerateTfIdf [--main|--sample]")
            sc.stop()
        }

        // Location of Data
        val data_loc_list = Map("main" -> "/data/stackOverflow2017/Posts.xml", "sample" -> "spark-search-engine/sample_data/Posts.xml")
        var data_loc = data_loc_list("sample")

        // Location of Sequence Files
        val index_loc_list = Map("main" -> "spark-search-engine/index", "sample" -> "spark-search-engine/sample_index")
        var index_loc = index_loc_list("sample")

        // sc.saveAsObjectFile(index_loc) // Save RDDs as Spark Objects (Sequence Files)
        // sc.objectFile(index_loc + "/") // Load Spark Objects (Sequence Files) as RDDs

        if(args(0) == "--main"){
            data_loc = data_loc_list("main")
            index_loc = index_loc_list("main")
        }
        else if(args(0) == "--sample"){
            data_loc = data_loc_list("sample")
            index_loc = index_loc_list("sample")
        }
        else {
            System.err.println("Usage: GenerateTfIdf [--main|--sample]")
        }

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
        // var tf_idf_set_opt_list = tf_idf_set.map(row => (row._2._2, row)).sortByKey(false).map(row => (row._2)).sortByKey()

        // Method 2 (Not Being Used): Resulting Dataset is (word, CompactBuffer((ID,TF-IDF), (ID,TF-IDF), (ID,TF-IDF), ..., (ID,TF-IDF)))
        // var tf_idf_set = tf_set_preJoin.join(idf_set).map(pre_tf_idf => (pre_tf_idf._1, (pre_tf_idf._2._1._1, (pre_tf_idf._2._1._2 * pre_tf_idf._2._2))))
        // var tf_idf_set_opt_cb= tf_idf_set.combineByKey()

        // Save TF-IDF set into HDFS
        idf_set.saveAsObjectFile(index_loc + "/idf") // Save IDF Set
        tf_idf_set.saveAsObjectFile(index_loc + "/tf_idf") // Save TF_IDF Set

        // Stop Spark
        sc.stop()
    }
}
