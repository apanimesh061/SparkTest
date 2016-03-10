package TopicApp

import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by Animesh Pandey 
  * on 3/10/16.
  */

trait SparkBase extends Serializable {
  protected def createSCLocal(name:String, config:HadoopConfig):SparkContext = {
    val iterator = config.iterator()
    for (prop <- iterator) {
      val k = prop.getKey
      val v = prop.getValue
      if (k.startsWith("spark."))
        System.setProperty(k, v)
    }
    val runtime = Runtime.getRuntime
    runtime.gc()

    val conf = new SparkConf()
    conf.setMaster("local[2]")

    conf.setAppName(name)
    conf.set("spark.serializer", classOf[KryoSerializer].getName)

    conf.set("spark.ui.port", "0")

    new SparkContext(conf)
  }
}
