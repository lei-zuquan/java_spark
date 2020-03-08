package com.scala.spark_core.accumulator_broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:31 上午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: 广播变量（调优策略）
 */

/*

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
比如，如果你的应用需要向所有节点发送一个较大的只读查询表，甚至是机器学习算法中的一个很大的特征向量，
广播变量用起来都很顺手。 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

使用广播变量的过程如下：
(1) 通过对一个类型 T 的对象调用 SparkContext.broadcast 创建出一个 Broadcast[T] 对象。 任何可序列化的类型都可以这么实现。
(2) 通过 value 属性访问该对象的值(在 Java 中为 value() 方法)。
(3) 变量只会被发到各个节点一次，应作为只读值处理(修改这个值不会影响到别的节点)。

 */
object Spark22_Broadcast {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark22_Broadcast")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    var rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

    // var rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    // val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2) //效率很差，设计到shuffle的过程，笛卡尔乘积，效率太慢
    // joinRDD.foreach(println)

    val list = List((1, 1), (2, 2), (3, 3))
    //可以使用广播变量减少数据的传输
    //构建广播变量
    var broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list) //共享的只读变量，减少数据传输的总量
    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map { //map没有shuffle
      case (key, value) => {
        var v2: Any = null
        //2，使用广播变量
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)
    //释放资源
    sc.stop()
  }

}
