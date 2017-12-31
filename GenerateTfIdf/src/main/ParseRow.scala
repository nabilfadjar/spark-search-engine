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
