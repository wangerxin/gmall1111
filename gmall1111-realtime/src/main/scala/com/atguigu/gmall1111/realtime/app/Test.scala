package com.atguigu.gmall1111.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1111.common.constant.GmallConstant
import com.atguigu.gmall1111.common.util.MyEsUtil
import com.atguigu.gmall1111.realtime.bean.StartUpLog
import com.atguigu.gmall1111.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("client_log",ssc)

    inputDStream.print()
    inputDStream.foreachRDD{ rdd=>{
        rdd.foreachPartition{ par=>
          par.foreach{line=>{
            print(line.toString)
          }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
