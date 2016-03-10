package TopicApp

import org.apache.hadoop.conf.Configuration


/**
  * Created by Animesh Pandey 
  * on 3/10/16.
  */
object TopicApp extends Serializable {

  def run() {

    val start = System.currentTimeMillis()

    val sparkConf = new Configuration()
    sparkConf.set("spark.executor.memory","1g")
    sparkConf.set("spark.kryoserializer.buffer","256")

    val es = new EsContext(sparkConf)
    val esConf = new Configuration()
    esConf.set("es.nodes","10.0.2.2")
    esConf.set("es.port","9220")
    esConf.set("es.resource", "traackr_alt_restored/post")
    esConf.set("es.query", "?q=*:*")
    esConf.set("es.fields", "_score,_id")

    val documents = es.documents(esConf)
    documents.foreach(println)

    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")

    es.shutdown()

  }

  def main(args: Array[String]) {
    run()
  }

}
