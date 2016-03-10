package TopicApp

import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.io.{ArrayWritable, MapWritable, NullWritable, Text}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import scala.collection.JavaConversions._

case class EsDocument(id:String,data:Map[String,String])

/**
  * Created by Animesh Pandey 
  * on 3/10/16.
  */
class EsContext(sparkConf:HadoopConfig) extends SparkBase {
  private val sc = createSCLocal("ElasticContext", sparkConf)

  def documentsAsJson(esConf:HadoopConfig):RDD[String] = {
    implicit val formats = DefaultFormats
    val source = sc.newAPIHadoopRDD(
      esConf,
      classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text],
      classOf[MapWritable]
    )
    val docs = source.map(
      hit => {
        val doc = Map("ident" -> hit._1.toString) ++ mwToMap(hit._2)
        write(doc)
      }
    )
    docs
  }

  def documents(esConf:HadoopConfig):RDD[EsDocument] = {
    val source = sc.newAPIHadoopRDD(
      esConf,
      classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text],
      classOf[MapWritable]
    )
    source.map(
      hit => new EsDocument(hit._1.toString, mwToMap(hit._2))
    )
  }

  def shutdown() = sc.stop()

  private def mwToMap(mw:MapWritable):Map[String, String] = {
    val m = mw.map(
      e => {
        val k = e._1.toString
        val v = e._2 match {
          case _: Text => e._2.toString

          case writable: ArrayWritable =>
            val array = writable.get()
            array.map(
              item => {
                if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString
              }
            ).mkString(",")

          case _ => ""
        }
        k -> v
      }
    )
    m.toMap
  }

  private def mwToMap(mw:MapWritable, fields:Array[String]):Map[String, String] = {
    val m = mw.map(
      e => {
        val k = e._1.toString
        val v = e._2 match {
          case _: Text =>
            e._2.toString

          case writable: ArrayWritable =>
            val array = writable.get()
            array.map(
              item => {
                if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString
              }
            ).mkString(",")

          case _ => ""
        }
        k -> v
      }
    )
    m.toMap
  }

  private def toVector(data:Map[String, String], fields:Array[String]):Vector = {
    val features = data.filter(kv => fields.contains(kv._1)).map(_._2.toDouble)
    Vectors.dense(features.toArray)
  }

}



