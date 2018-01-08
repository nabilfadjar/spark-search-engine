#
# Executes the project via Fabric
#

from fabric.api import local, env, run, prompt
from fabric.context_managers import lcd

#
# Packaging
#

def package():
    print("Packaging all Scala Projects into JAR files")
    package_tf_idf()
    package_query_engine()

def package_tf_idf():
    print("Packaging TF-IDF into JAR files...")
    with lcd('GenerateTfIdf/'):
        local("sbt clean package")

def package_query_engine():
    print("Packaging Query Engine into JAR file...")
    with lcd('QueryEngine/'):
        local("sbt clean package")

def package_tf_idf_query_combo():
    print("Packaging TF-IDF / Query Engine Combo into JAR file...")
    with lcd('TfIdfQueryEnigneCombined/'):
        local("sbt clean package")

#
# Execution
#

def run_tf_idf():
    print("[MAIN] Sumbitting job to Spark for Generating TF-IDF...")
    with lcd('GenerateTfIdf/'):
        local("spark-submit target/scala-2.10/generatetfidf_2.10-1.0.0-SNAPSHOT.jar --main >> logs/spark.log 2>&1 &")

def run_sample_tf_idf():
    print("[SAMPLE] Sumbitting job to Spark for Generating TF-IDF...")
    with lcd('GenerateTfIdf/'):
        local("spark-submit target/scala-2.10/generatetfidf_2.10-1.0.0-SNAPSHOT.jar --sample >> logs/spark.log 2>&1 &")

def run_query_engine():
    print("[MAIN] Sumbitting job to Spark for Query Engine...")
    query = prompt("Query?")
    with lcd('QueryEngine/'):
        local("spark-submit target/scala-2.10/queryengine_2.10-1.0.0-SNAPSHOT.jar --main \"%s\" >> logs/spark.log 2>&1 &" % (query))

def run_sample_query_engine():
    print("[SAMPLE] Sumbitting job to Spark for Query Engine...")
    query = prompt("Query?")
    with lcd('QueryEngine/'):
        local("spark-submit target/scala-2.10/queryengine_2.10-1.0.0-SNAPSHOT.jar --sample \"%s\" >> logs/spark.log 2>&1 &" % (query))

def run_tf_idf_query_combo():
    print("[MAIN] Sumbitting job to Spark for TF-IDF / Query Engine Combo...")
    query = prompt("Query?")
    with lcd('TfIdfQueryEnigneCombined/'):
        local("spark-submit target/scala-2.10/tfidfqueryenignecombined_2.10-1.0.0-SNAPSHOT.jar --main \"%s\" >> logs/spark.log 2>&1 &" % (query))

def run_sample_tf_idf_query_combo():
    print("[SAMPLE] Sumbitting job to Spark for TF-IDF / Query Engine Combo...")
    query = prompt("Query?")
    with lcd('TfIdfQueryEnigneCombined/'):
        local("spark-submit target/scala-2.10/tfidfqueryenignecombined_2.10-1.0.0-SNAPSHOT.jar --sample \"%s\" >> logs/spark.log 2>&1 &" % (query))


#
# HDFS Commands
#

def hdfs_ls_query_engine():
    print("Listing files in HDFS in spark-search-engine...")
    local("hadoop fs -ls /user/mnf30")

def hdfs_check_quota():
    print("HDFS: Quota")
    local("hdfs dfs -count -q -h /user/mnf30")

def hdfs_clear_query_results():
    print("Clearing processed results...")
    local("hadoop fs -rm -R -skipTrash spark-search-engine/results/search_results_id")
    local("hadoop fs -rm -R -skipTrash spark-search-engine/results/search_results_xml")

def hdfs_clear_sample_query_results():
    print("Clearing processed results...")
    local("hadoop fs -rm -R -skipTrash spark-search-engine/sample_results/search_results_id")
    local("hadoop fs -rm -R -skipTrash spark-search-engine/sample_results/search_results_xml")
