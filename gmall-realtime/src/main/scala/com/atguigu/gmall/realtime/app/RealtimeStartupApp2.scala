package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.StartUpLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, streamingContext)


//    // 构建流式数据处理环境


    startupStream.foreachRDD{rdd =>{
      println(rdd.map(_.value()).collect().mkString("\n"))
    }}
//
    //转换为类

    val startupLogDstream = startupStream.map(_.value()).map { log => {
      val logeerObejct: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      val date = new Date(logeerObejct.ts)
      val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      val dateArr = dateStr.split(" ")
      logeerObejct.logDate = dateArr(1)
      logeerObejct.logHourMinute = dateArr(2)
      val strings = dateArr(1).split(":")
      logeerObejct.logHour = strings(0)
      logeerObejct
    }
    }

    //过滤

    val dauFilterDStream: DStream[StartUpLog] = startupLogDstream.transform(rdd => {
      val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedisClient = RedisUtil.getJedisClient
      val key = "dau:" + dateStr
      val dauSetAll: util.Set[String] = jedisClient.smembers(key)
      val likelike: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(dauSetAll)
      jedisClient.close()
      val daufilter = rdd.filter { rrd =>
        val dauList: util.Set[String] = likelike.value
        !dauList.contains(rrd.mid)
      }
      daufilter
    })

    //内部过滤

    val daugroupByKey: DStream[(String, Iterable[StartUpLog])] = dauFilterDStream.map { rdd =>(rdd.mid, rdd)}.groupByKey()
    val dauFlatMap: DStream[StartUpLog] = daugroupByKey.flatMap {
      case (mid, log) => {
        log.take(1)
      }
    }



    //保存

    dauFlatMap.foreachRDD(rdd => {

      rdd.foreachPartition(datas => {
        val jedisClient = RedisUtil.getJedisClient
        datas.foreach { data =>
          val key = "dau" + data.logDate
          val value = data.mid
          jedisClient.sadd(key, value)
        }
        jedisClient.close()
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
