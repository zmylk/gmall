package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import com.atguigu.gmall.util.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //保存到ES
    //数据脱敏，补充时间戳

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    val inputDstream: DStream[OrderInfo] = kafkaDStream.map { record => {
      //接受转化JSON对象
      val jsonValue = record.value()
      val jsonObject: OrderInfo = JSON.parseObject(jsonValue, classOf[OrderInfo])

      //脱敏
      val tuple: (String, String) = jsonObject.consigneeTel.splitAt(4)
      jsonObject.consigneeTel = tuple._1 + "*******"

      //补充数据
      val datetimeArr: Array[String] = jsonObject.createTime.split(" ")
      jsonObject.createDate = datetimeArr(0)
      val timeArr = datetimeArr(1).split(":")
      jsonObject.createHour = timeArr(0)
      jsonObject.createHourMinute = timeArr(0) + ":" + timeArr(1)
      jsonObject
    }
    }
    inputDstream.foreachRDD{rdd =>{

      rdd.foreachPartition(date =>{
        MyEsUtil.indexBulk(GmallConstants.ES_INDEX_ORDER,date.toList)
      })
    }}

    ssc.start()
    ssc.awaitTermination()
  }

}
