# Action Plan

## Big Data Engine
We are using Apache Spark with Scala for processing our data set.

## Data Set
The data set being used is the Stack Overflow Data set from 2017. This can be found in QMUL's HDFS at `/data/stackOverflow2017`.

More information on the data set can be found in this [link](https://archive.org/details/stackexchange).

More metadata related information can be found in this [link](https://archive.org/download/stackexchange/readme.txt).

## Parsing XML Data
Originaly, we were given a sample Java code to parse XML into a map data structure:
> The following util class has the transformXmlToMap(String xml) method that simplifies parsing of stackoverflow data format: https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/utils/MRDPUtils.java

Since we were using Scala to develop our search engine, we decided to apply the same concept of parsing XML into a map data structure into Scala code.

The following Scala code shows our implementation of mapping XML into a map data structure:
```scala
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
}
```

The next section would explain how we filtered the XML with the code above to produce a cleaner data set.

## Data Cleansing
We need to check if all data in the dataset is valid and can be parsed by our code. Ideally, we would like to parse each row into its own class and access each attribute within the class via a map. Thus, the approach we are taking would be to run several checks on each of our dataset.

#### Step 1: Check if dataset follows XML Format
We want to check whether the dataset that we have follows the same format as we have envisioned. For example, we would like the `Posts.xml` dataset to follow the similar format:
```xml
<?xml version="1.0" encoding="utf-8"?>
<posts>
    <row Id="4" PostTypeId="1" ... />
    <row Id="6" PostTypeId="1" ... />
    ...
    <row Id="7" PostTypeId="2" ... />
</post>
```
These are the set of rules we can apply:
 - Check if the XML file starts with the approriate header and ends with the approriate footer. The header could be the XML version as well as the opening and closing tags of the xml.
 - Check if each row contains a valid and unique ID. This approach can be done be collating all the row IDs as key/value pair and check if the length of the result set of key/value pair is equal the the number of posts given (length of the number of valid posts rows)


## Inverted Index Data set
This step is crucial for a search engine for allowing fast access to the data set essentially acheiving a near `O(1)` time complexitiy.

The Inverted Index data set would be produced through implementating TF-IDF (Term Frequency - Inverse Document Frequency)

Fore more information, visit this [link](http://www.tfidf.com/).

## SBT Packaging
Open the following links:
https://alvinalexander.com/scala/how-to-create-sbt-project-directory-structure-scala
https://alvinalexander.com/scala/sbt-how-to-compile-run-package-scala-project
