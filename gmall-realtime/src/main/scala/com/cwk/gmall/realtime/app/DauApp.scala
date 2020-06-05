package com.cwk.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import util.Date

import com.alibaba.fastjson.JSON
import com.cwk.common.util.MyEsUtil
import com.cwk.gmall.bean.Startuplog
import com.cwk.gmall.commom.constant.GmallConstant
import com.cwk.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.cwk.gmall.commom

object DauApp {

  def main(args: Array[String]): Unit = {


    val Sparkconf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(Sparkconf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)
/*
    inputDstream.foreachRDD{

      rdd =>
        println(rdd.map(_.value()).collect().mkString("\n"))


    }*/

    //转换处理
    val startuplogStream: DStream[Startuplog] = inputDstream.map {


      record =>

        val jsonString: String = record.value()
        val startuplog: Startuplog = JSON.parseObject(jsonString, classOf[Startuplog])
        val date = new Date(startuplog.ts)

        val dateString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr: Array[String] = dateString.split(" ")
        startuplog.logDate = dateArr(0)
        startuplog.logHour = dateArr(1).split(":")(0)
        startuplog.logHourMinute = dateArr(1)

        startuplog
    }



    // 利用redis进行去重过滤
    val filteredDstream: DStream[Startuplog] =
      startuplogStream.transform {
      rdd =>
        println("过滤前："+rdd.count())
      //driver  //周期性执行
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau:" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filteredRDD: RDD[Startuplog] = rdd.filter { startuplog =>
        //executor
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.mid)
      }
      println("过滤后："+filteredRDD.count())
      filteredRDD

    }

   //去重思路;把相同的mid的数据分成一组 ，每组取第一个
   val groupByMidDstream: DStream[(String, Iterable[Startuplog])] = filteredDstream.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
    val distincDStream: DStream[Startuplog] = groupByMidDstream.flatMap {
      case (mid, startulogItr) =>
        startulogItr.take(1)

    }



    //    保存到redis
    distincDStream.foreachRDD{
      //driver
      //redis type set
      //key dau:2019-06-03 value:mids
      rdd =>
        rdd.foreachPartition{startupLogItr =>

          //executor
          val jedis: Jedis = RedisUtil.getJedisClient
          val list:List[Startuplog] = startupLogItr.toList
          for (startlog <- startupLogItr) {
            val key = "dau:"+ startlog.logDate
            val value = startlog.mid
            jedis.sadd(key, value)
            println(startlog)

          }
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_DAU,list)
          jedis.close()


        }
    }
    distincDStream.foreachRDD(println(_))

    ssc.start()
    ssc.awaitTermination()

  }

}
