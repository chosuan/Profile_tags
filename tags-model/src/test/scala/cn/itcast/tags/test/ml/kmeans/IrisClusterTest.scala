package cn.itcast.tags.test.ml.kmeans

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用KMeans算法对鸢尾花数据进行聚类操作
 */
object IrisClusterTest {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 1. 读取鸢尾花数据集
        val irisDF: DataFrame = spark.read
            .format("libsvm")
            .load("datas\\iris\\iris_kmeans.txt")

        /*
        root
         |-- label: double (nullable = true)
         |-- features: vector (nullable = true)
         */
//        irisDF.printSchema()
        /*
            +-----+-------------------------------+
            |label|features                       |  稠密向量：(5.1, 3.5, 1.4, 0.2)
            +-----+-------------------------------+
            |1.0  |(4,[0,1,2,3],[5.1,3.5,1.4,0.2])|   -> 稀疏向量表示法
            |1.0  |(4,[0,1,2,3],[4.9,3.0,1.4,0.2])|
         */
//        irisDF.show( 20 ,false)

        // 2. 创建KMeans模型学习器对象（算法）
        val kMeans: KMeans = new KMeans()
            // 设置输入列和预测之列名称
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            // 设置算法相关超参数的值
            .setK(3)
            .setMaxIter(20)
            .setInitMode("k-means||")

        // 3. 使用数据集训练模型
        val kMeansModel: KMeansModel = kMeans.fit(irisDF)

        // 4. 模型评估，计算误差平方和SSE
        val wssse: Double = kMeansModel.computeCost(irisDF)
        // WSSSE : 78.94506582597637
//        println(s"WSSSE =  ${wssse}")

        // 5. 获取类簇中心点
        val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
        // 将类簇中心点采用拉链操作 获取索引
        /*
        ([5.88360655737705,2.7409836065573776,4.388524590163936,1.4344262295081969],0)
        ([5.005999999999999,3.4180000000000006,1.4640000000000002,0.2439999999999999],1)
        ([6.853846153846153,3.0769230769230766,5.715384615384615,2.053846153846153],2)
         */
        clusterCenters.zipWithIndex.foreach(println)

        // 6 预测数据 所属类簇
        val predictionDF: DataFrame = kMeansModel.transform(irisDF)
        /*
            root
             |-- label: double (nullable = true)
             |-- features: vector (nullable = true)
             |-- prediction: integer (nullable = true)
         */
//        predictionDF.printSchema()
        /*
        +-----+-------------------------------+----------+
        |label|features                       |prediction|
        +-----+-------------------------------+----------+
        |1.0  |(4,[0,1,2,3],[5.1,3.5,1.4,0.2])|1         |
        |1.0  |(4,[0,1,2,3],[4.9,3.0,1.4,0.2])|1         |
        |1.0  |(4,[0,1,2,3],[4.7,3.2,1.3,0.2])|1         |
         */
//        predictionDF.show(20,false)

        //查看 模型预测 统计信息  看出该模型的准确度
        predictionDF
            .groupBy($"label", $"prediction")
            .count()
            .show(10, truncate = false)
        /*
        +-----+----------+-----+
        |label|prediction|count|
        +-----+----------+-----+
        |1.0  |1         |50   |
        |3.0  |0         |14   |
        |2.0  |2         |3    |
        |2.0  |0         |47   |
        |3.0  |2         |36   |
        +-----+----------+-----+
         */

        // 应用结束， 关闭资源
        spark.stop()

    }

}
