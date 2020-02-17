package com.like.tryone.app

import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall.util.MyEsUtil
import com.like.tryone.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ExportSaleApp {
  def main(args: Array[String]): Unit = {
    var date = ""
    if (args != null&&args.length>0)
      {
        date = args(0)
      }
    else {
      date="2020-02-10"
    }


    val conf = new SparkConf().setAppName("sale_app").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import session.implicits._
    session.sql("use sparkmall2020")
    val saleDetailDaycountRDD: RDD[SaleDetailDaycount] = session.sql("select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast(sku_price as double),sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt" +
      " from dws_sale_detail_daycount where dt='" + date + "'").as[SaleDetailDaycount].rdd


    saleDetailDaycountRDD.foreachPartition{saleItr=>{
      val listBuffer: ListBuffer[SaleDetailDaycount] = ListBuffer()
      for (elem <- saleItr) {
        listBuffer+=elem
        if (listBuffer.size == 100)
          {
            MyEsUtil.indexBulk(GmallConstants.ES_INDEX_SALE,listBuffer.toList)
            listBuffer.clear()
          }
      }
      if (listBuffer.size >0)
        {
          MyEsUtil.indexBulk(GmallConstants.ES_INDEX_SALE,listBuffer.toList)
        }
    }}


  }

}
