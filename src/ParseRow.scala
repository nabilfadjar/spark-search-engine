class Post(val toBeParsed: String) {
    private var postMap = if(isHeaderOrFooter()) null else transformIntoMap()

    private def transformIntoMap() : Map[String, String] = {
        // var preParsedRow = toBeParsed.split('"').map(_.replace("<row","")).map(_.replace("/>","")).map(_.trim).filterNot(_.isEmpty)
        // var postHeader = preParsedRow.filter(_.endsWith("=")).map(_.replace("=", ""))
        // var postContent = preParsedRow.filterNot(_.endsWith("="))
        // return (postHeader zip postContent).toMap
        return toBeParsed.split("(=\")|(\"[\\s])|(<[\\w]*)|(/>)").map(_.trim).filter(_.nonEmpty).grouped(2).collect { case Array(k, v) => k -> v }.toMap
    }

    private def isHeaderOrFooter() : Boolean = {
        return (toBeParsed.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>") || toBeParsed.endsWith("posts>"))
    }

    def getMap() : Map[String,String] = {
        return postMap
    }

    def getId() : Int = {
        return postMap.get("Id").getOrElse("-1").toInt
    }

    def getBody() : String = {
        return postMap.get("Body").getOrElse(null)
    }

    def getWordsFromBody() : Array[String] = {
        if (getBody() == null) return null else return getBody().toLowerCase.replaceAll("&lt;code&gt;", "").replaceAll("(&[\\S]*;)|(&lt;[\\S]*&gt;)", " ").replaceAll("[\\s](a href)|(rel)[\\s]", " ").replaceAll("(?!([\\w]*'[\\w]))([\\W_\\s\\d])+"," ").split(" ").filter(_.nonEmpty)
    }
}
