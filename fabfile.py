#
# Executes the project via Fabric
#

from fabric.api import local, env, run
from fabric.context_managers import lcd

def package():
    print("Packaging all Scala Projects into JAR files")
    package_tf_idf()
    package_query_engine()

def package_tf_idf():
    print("Packaging TF-IDF into JAR files...")
    with lcd('GenerateTfIdf/'):
        local("sbt clean package")

def package_query_engine():
    print("Packaging all Query Engine into JAR file...")
    with lcd('QueryEngine/'):
        local("sbt clean package")

def run_tf_idf():
    print("[MAIN] Sumbitting job to Spark for Generating TF-IDF...")
    with lcd('GenerateTfIdf/target/scala-2.10'):
        local("spark-submit generatetfidf_2.10-1.0.0-SNAPSHOT.jar --main >> logs/spark.log &")

def run_sample_tf_idf():
    print("[SAMPLE] Sumbitting job to Spark for Generating TF-IDF...")
    with lcd('GenerateTfIdf/target/scala-2.10'):
        local("spark-submit generatetfidf_2.10-1.0.0-SNAPSHOT.jar --sample >> logs/spark.log &")

def run_query_engine():
    print("[MAIN] Sumbitting job to Spark for Query Engine...")
    query = prompt("Query?")
    with lcd('QueryEngine/target/scala-2.10'):
        local("spark-submit queryengine_2.10-1.0.0-SNAPSHOT.jar --main \"%s\" >> logs/spark.log &" % (query) )

def run_sample_query_engine():
    print("[SAMPLE] Packaging all Query Engine into Query Engine...")
    query = prompt("Query?")
    with lcd('QueryEngine/target/scala-2.10'):
        local("spark-submit queryengine_2.10-1.0.0-SNAPSHOT.jar --sample \"%s\" >> logs/spark.log &" % (query))
