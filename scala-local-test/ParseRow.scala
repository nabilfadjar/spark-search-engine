#!/usr/bin/env scala

import scala.io.Source

class Post(val toBeParsed: String) {
    private var postMap = if(isHeaderOrFooter()) null else transformIntoMap()

    private def transformIntoMap() : Map[String, String] = {
        var preParsedRow = toBeParsed.split('"').map(_.replace("<row","")).map(_.replace("/>","")).map(_.trim).filterNot(_.isEmpty)
        var postHeader = preParsedRow.filter(_.endsWith("=")).map(_.replace("=", ""))
        var postContent = preParsedRow.filterNot(_.endsWith("="))
        return (postHeader zip postContent).toMap
    }

    private def isHeaderOrFooter() : Boolean = {
        return (toBeParsed.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>") || toBeParsed.endsWith("posts>"))
    }

    def getPost() :Map[String,String] = {
        return postMap
    }

    def getId() : Int = {
        return postMap.get("Id").getOrElse("-1").toInt
    }

    def getBody() : String = {
        return postMap.get("Body").getOrElse(null)
    }
}

def readXMLFile() : Array[String] = {
    val xmlSource = Source.fromFile("../sample_data/Posts.xml")
    val posts = xmlSource.getLines.toArray
    xmlSource.close
    return posts
}

// Quick Code
// var posts = readXMLFile.map(row => new Post(row)).filterNot(_.getPost() == null)
// var wordTuple = posts.flatMap(_.getBody.split(" ")).map(word => (word,1)).reduceByKey(word,count => (word+count))
// var idf = (Math.log(totalPosts) - Math.log(wordCount))/Math.log(Math.E)
// var idfArr = reduced.map(eachTuple => (Math.log(totalPosts) - Math.log(eachTuple._2))/Math.log(Math.E))

// Regex
//sampleBody.replaceAll("(&[\\S]*;)|(&lt;[\\S]*&gt;)", " ").replaceAll("(a href)|(rel)", " ").replaceAll("[\\W\\s\\d]"," ").split(" ").filter(_.nonEmpty)
