package com.example.rss

import scala.concurrent.duration._
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import com.example.rss.persistence.SimpleMongoWrapper
import com.example.rss.model.NewsEntry
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONDocument

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object RSSDemo {
  def main(args: Array[String]): Unit = {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
    
    val urls = Array(
      "http://rss.cnn.com/rss/edition.rss",
      "http://rss.cnn.com/rss/edition_world.rss",
      "http://rss.cnn.com/rss/edition_africa.rss",
      "http://rss.cnn.com/rss/edition_americas.rss",
      "http://rss.cnn.com/rss/edition_asia.rss",
      "http://rss.cnn.com/rss/edition_europe.rss",
      "http://rss.cnn.com/rss/edition_meast.rss",
      "http://rss.cnn.com/rss/edition_us.rss",
      "http://rss.cnn.com/rss/money_news_international.rss",
      "http://rss.cnn.com/rss/edition_technology.rss",
      "http://rss.cnn.com/rss/edition_space.rss",
      "http://rss.cnn.com/rss/edition_entertainment.rss",
      "http://rss.cnn.com/rss/edition_sport.rss",
      "http://rss.cnn.com/rss/edition_football.rss",
      "http://rss.cnn.com/rss/edition_golf.rss",
      "http://rss.cnn.com/rss/edition_motorsport.rss",
      "http://rss.cnn.com/rss/edition_tennis.rss",
      "http://rss.cnn.com/rss/edition_travel.rss",
      "http://rss.cnn.com/rss/cnn_freevideo.rss",
      "http://rss.cnn.com/rss/cnn_latest.rss"
      )
    val newsStream = new RSSInputDStream(urls, Map(
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)

    var lastNewsRDD: Option[RDD[(String, RSSEntry)]] = None

    val partitioner = new HashPartitioner(2*sc.defaultParallelism)

    val mongoUri = "mongodb://root:example@db:27017/test?authSource=admin"
    val mongo = new SimpleMongoWrapper(mongoUri, "news-db")

    def transformToClose[T](f: Future[T])(closeFunc: () => Unit)(implicit executor: ExecutionContext): Future[Any] =
      f.transform({_ => closeFunc()}, {t => closeFunc();t})

    newsStream.window(Minutes(60), Seconds(30)) foreachRDD { rdd =>
      val newsRDD = rdd
        .flatMap(rssEntry => {
          val words = rssEntry.title
            .split("\\s+")
            .map(_.filterNot(",.:'!\"@#$%^&*()".contains(_)))
            .filter(word => !Set("on", "in", "and", "vs", "to", "about", "the").contains(word.toLowerCase))
          words.map(word => (word, rssEntry))
        })
        .partitionBy(partitioner)
        .cache
      lastNewsRDD = Some(newsRDD)
    }

    val trendsStream = new RSSInputDStream(Array("https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"), Map(
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)


    trendsStream.window(Minutes(60), Seconds(30)) foreachRDD { rdd =>
      val spark = SparkSession.builder.appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
      if (!rdd.isEmpty) {
        val trendsRDD = rdd
            .flatMap(_.title.split("\\s+")
              .map(_.filterNot(",.:'!\"@#$%^&*()".contains(_)))
              .filterNot(word => Set("on", "in", "and", "vs", "to", "about", "the").contains(word.toLowerCase))
              .toSet)
            .map(trend => (trend, trend))
            .partitionBy(partitioner)
            .cache
        lastNewsRDD match {
          case Some(newsRDD) =>
            val joinedRDD = newsRDD.join(trendsRDD)
            val keysByEntriesRDD = joinedRDD map { t: (String, (RSSEntry, String)) => t match {
                  case (key, (rssEntry, trend)) =>
                    (rssEntry, key)
                }
              }
            val resultRDD = keysByEntriesRDD
              .groupByKey
              .mapValues(_.toList)
              .map {
                case (rssEntry: RSSEntry, tags: List[String]) =>
                  NewsEntry(rssEntry.title, rssEntry.links.map(_.href).mkString(","), rssEntry.publishedDate, tags)
              }
            val mongoInDriver = mongo.copy
            import scala.concurrent.ExecutionContext.Implicits.global
            val futureRemove: Future[WriteResult] = mongoInDriver.collection("news").flatMap(_.remove(BSONDocument()))
            futureRemove onComplete {
              case Success(writeResult) if writeResult.ok =>
                resultRDD.foreachPartition { partition =>
                  partition foreach { newsEntry =>
                      val mongoInPartition = mongo.copy
                      val newsCollectionFuture = mongoInPartition.collection("news")
                      val futureInsert = newsCollectionFuture.flatMap(_.insert(newsEntry))
                      futureInsert onComplete { result =>
                        println(s"future insert done: $result")
                      }
                      transformToClose(futureInsert) { () =>
                        mongoInPartition.futureConnection foreach { conn =>
                          conn.askClose()(10.seconds)
                        }
                      }
                  }
                }
            }
            transformToClose(futureRemove) { () =>
              mongoInDriver.futureConnection foreach { conn =>
                conn.askClose()(10.seconds)
              }
            }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
