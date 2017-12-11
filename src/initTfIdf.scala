object initTfIdf extends App {
    // Location of Data
    val sample_data_loc = "spark-search-engine/sample_data/Posts.xml"
    // val main_data_loc = "/data/stackOverflow2017/Posts.xml"

    // Parse XML posts as Post Objects
    var posts = sc.textFile(sample_data_loc).map(row => new Post(row)).filter(_.getMap() != null)
    val posts_count = sc.broadcast(posts.count().toDouble)

    // Create Word Tuple for Word Count
    var wordTuple = posts.flatMap(_.getWordsFromBody()).map(word => (word,1)).reduceByKey((a,b) => (a+b))

    // Generate TF Set
    var tf_set = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => ((word, eachPost.getId), 1.0/eachPost.getNumberOfWordsInPost))).reduceByKey((a,b) => (a+b))
    // var wordInPostTuple = posts.flatMap(eachPost => eachPost.getWordsFromBody().map(word => (word, eachPost.getId) )).map(wordInPostKey => (wordInPostKey,1)).reduceByKey((a,b) => (a+b))

    // Generate IDF Set
    var idf_set = wordTuple.map(eachWordTuple => (eachWordTuple._1,(Math.log(posts_count.value) - Math.log(eachWordTuple._2))/Math.log(Math.E)))

    // Generate TF-IDF Set
    var tf-idf_set = tf_set.map()
}
