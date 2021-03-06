package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.StartUpLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.gmall.util.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //           startupStream.map(_.value()).foreachRDD{ rdd=>
    //             println(rdd.collect().mkString("\n"))
    //           }

    //转换为类

    val startupLogDstream = startupStream.map(_.value()).map { log => {
      val logeerObejct: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      val date = new Date(logeerObejct.ts)
      val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      val dateArr = dateStr.split(" ")
      logeerObejct.logDate = dateArr(0)
      logeerObejct.logHourMinute = dateArr(1)
      val strings = dateArr(1).split(":")
      logeerObejct.logHour = strings(0)
      logeerObejct
    }
    }

    //过滤

    val dauFilterDStream: DStream[StartUpLog] = startupLogDstream.transform(rdd => {
      //println("第一处，count="+rdd.count())
      val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedisClient = RedisUtil.getJedisClient
      val key = "dau:" + dateStr
      val dauSetAll: util.Set[String] = jedisClient.smembers(key)
      val likelike: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSetAll)
      jedisClient.close()
      val daufilter = rdd.filter { rrd =>
        val dauList: util.Set[String] = likelike.value
        !dauList.contains(rrd.mid)
      }
      //println("第二处，count="+daufilter.count())
      daufilter
    })

    //内部过滤


    val daugroupByKey: DStream[(String, Iterable[StartUpLog])] = dauFilterDStream.map { rdd => (rdd.mid, rdd) }.groupByKey()
    val dauFlatMap: DStream[StartUpLog] = daugroupByKey.flatMap {
      case (mid, log) => {
        log.take(1)
      }
    }



    //保存

    dauFlatMap.foreachRDD(rdd => {

      //println("第三处，count="+rdd.count())
      rdd.foreachPartition(datas => {

        //可迭代集合只能使用一次


        val jedisClient = RedisUtil.getJedisClient
        val list: List[StartUpLog] = datas.toList
        list.foreach { data =>
          val key = "dau:" + data.logDate
          val value = data.mid
          jedisClient.sadd(key, value)
        }
        //存入ES
        val str = "{\"area\":\"test03\",\"uid\":\"41\",\"itemid\":27,\"npgid\":36,\"evid\":\"addFavor\",\"os\":\"andriod\",\"pgid\":14,\"appid\":\"gmall12138\",\"mid\":\"mid_418\",\"type\":\"event\"}"

        MyEsUtil.indexBulk(GmallConstants.ES_INDEX_DAU,list)
        jedisClient.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
