package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
 * 挖掘类型标签模型开发：用户客户价值标签模型（RFM模型）
 */
class RfmModel extends AbstractModel("客户价值标签", ModelType.ML){
    /*
    358	客户价值
      359	  高价值		      0
      360	  中上价值	    	1
      361	  中价值		      2
      362	  中下价值	    	3
      363	  超低价值	    	4
     */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

        /*
          root
           |-- memberid: string (nullable = true)
           |-- ordersn: string (nullable = true)
           |-- orderamount: string (nullable = true)
           |-- finishtime: string (nullable = true)
         */
        //businessDF.printSchema()
        /*
          +--------+----------+-----------+----------+
          |memberid|   ordersn|orderamount|finishtime|
          +--------+----------+-----------+----------+
          |     263|gome_79...|    2479.45|1590249600|
          |     126|jd_1409...|    2449.00|1591545600|
          |     721|jd_1409...|    1099.42|1590595200|
          |     937|amazon_...|    1999.00|1591718400|
         */
        //businessDF.show(10, truncate = 10)

        val spark: SparkSession = businessDF.sparkSession
        import spark.implicits._

        // 1）、从订单数据获取字段值，计算每个用户RFM值
        val rfmDF: DataFrame = businessDF
            // 按照用户进行分组
            .groupBy($"memberid")
            // 聚合计算R、F、M值
            .agg(
                // R 值
                max($"finishtime").as("finish_time"), //
                // F 值
                count($"ordersn").as("frequency"), //
                // M 值
                sum($"orderamount".cast(DataTypes.createDecimalType(10, 2))).as("monetary")
            )
            .select(
                $"memberid".as("uid"),
                datediff(current_timestamp(), from_unixtime($"finish_time")).as("recency"),
                $"frequency", $"monetary"
            )
        /*
          root
           |-- uid: string (nullable = true)
           |-- recency: integer (nullable = true)
           |-- frequency: long (nullable = false)
           |-- monetary: decimal(20,2) (nullable = true)
         */
        //rfmDF.printSchema()
        /*
          +---+-------+---------+-----------+
          |uid|recency|frequency|monetary   |
          +---+-------+---------+-----------+
          |1  |77     |236      |544787.51  |
          |102|77     |94       |329543.67  |
          |110|77     |102      |152525.17  |
         */
        //rfmDF.show(130, truncate = false)

        // 2）、按照规则，给RFM值打分Score
        /*
          R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
          F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
          M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
         */
        // R 打分判断条件
        val rWhen: Column = when($"recency".between(1, 3), 5.0)
            .when($"recency".between(4, 6), 4.0)
            .when($"recency".between(7, 9), 3.0)
            .when($"recency".between(10, 15), 2.0)
            .when($"recency".gt(16), 1.0)
        // F 打分条件表达式
        val fWhen = when(col("frequency").between(1, 49), 1.0) //
            .when(col("frequency").between(50, 99), 2.0) //
            .when(col("frequency").between(100, 149), 3.0) //
            .when(col("frequency").between(150, 199), 4.0) //
            .when(col("frequency").geq(200), 5.0) //
        // M 打分条件表达式
        val mWhen = when(col("monetary").lt(10000), 1.0) //
            .when(col("monetary").between(10000, 49999), 2.0) //
            .when(col("monetary").between(50000, 99999), 3.0) //
            .when(col("monetary").between(100000, 199999), 4.0) //
            .when(col("monetary").geq(200000), 5.0) //
        // 基于规则对RFM值进行打分
        val rfmScoreDF: DataFrame = rfmDF
            .select(
                $"uid", rWhen.as("r_score"), //
                fWhen.as("f_score"), mWhen.as("m_score")
            )
        /*
          root
           |-- uid: string (nullable = true)
           |-- r_score: double (nullable = true)
           |-- f_score: double (nullable = true)
           |-- m_score: double (nullable = true)
         */
        //rfmScoreDF.printSchema()
        /*
          +---+-------+-------+-------+
          |uid|r_score|f_score|m_score|
          +---+-------+-------+-------+
          |1  |1.0    |5.0    |5.0    |
          |102|1.0    |2.0    |5.0    |
          |107|1.0    |3.0    |4.0    |
          |110|1.0    |3.0    |4.0    |
         */
        // rfmScoreDF.show(30, truncate = false)

        // 3）、将RFM数据使用KMeans算法聚类（K=5个）
        // 3.1 组合r、f、m到特征向量vector中
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("r_score", "f_score", "m_score"))
            .setOutputCol("features")
        val featuresDF: DataFrame = assembler.transform(rfmScoreDF)
        /*
          root
           |-- uid: string (nullable = true)
           |-- r_score: double (nullable = true)
           |-- f_score: double (nullable = true)
           |-- m_score: double (nullable = true)
           |-- features: vector (nullable = true)
         */
        //featuresDF.printSchema()
        /*
           +---+-------+-------+-------+-------------+
          |uid|r_score|f_score|m_score|features     |
          +---+-------+-------+-------+-------------+
          |1  |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|
          |102|1.0    |2.0    |5.0    |[1.0,2.0,5.0]|
          |107|1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
         */
        //featuresDF.show(10, truncate = false)

        // 3.2 使用KMeans算法，训练模型
        val kMeansModel: KMeansModel = trainModel(featuresDF)

        // 3.3 获取类簇中心点
        val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
        // 将类簇中心点，采用拉链操作，获取索引
        /*
          ([1.0, 3.0, 4.0],  0)
          ([1.0, 3.0, 5.0],  1)
          ([1.0, 5.0, 5.0],  2)
          ([1.0, 2.0, 4.0],  3)
          ([1.0, 2.0, 5.0],  4)
          --------------------------------
          (8.0, 0)
          (9.0, 1)
          (11.0,2)
          (7.0, 3)
          (8.0, 4)
          --------------------------------
          (11.0,2)
          (9.0, 1)
          (8.0, 0)
          (8.0, 4)
          (7.0, 3)
          --------------------------------
          ((11.0,2), 0)
          ((9.0, 1), 1)
          ((8.0, 0), 2)
          ((8.0, 4), 3)
          ((7.0, 3), 4)
         */
        val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
            .zipWithIndex
            // 获取类簇中心点向量之和（RFM之和）
            .map{case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex)}
            // rfm和降序排序
            .sortBy{case (rfm, _) => - rfm}
            // 拉链，获取索引
            .zipWithIndex
        //.foreach(println)

        // 3.4 使用模型对数据进行预测：划分类簇，属于哪个类别
        val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
        /*
          root
           |-- uid: string (nullable = true)
           |-- r_score: double (nullable = true)
           |-- f_score: double (nullable = true)
           |-- m_score: double (nullable = true)
           |-- features: vector (nullable = true)
           |-- prediction: integer (nullable = true)
         */
        //predictionDF.printSchema()
        /*
          +---+----------+-------+-------+-------+-------------+
          |uid|prediction|r_score|f_score|m_score|features     |
          +---+----------+-------+-------+-------+-------------+
          |1  |2         |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|
          |102|4         |1.0    |2.0    |5.0    |[1.0,2.0,5.0]|
          |107|0         |1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
          |110|0         |1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
          +---+----------+-------+-------+-------+-------------+
         */
        //predictionDF.show(20, truncate = false)

        // 4）、从KMeans中获取出每个用户属于簇
        // 4.1 获取属性标签数据：rule, tagId
        val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)

        // 4.2 对类簇中心点数据进行遍历，获取对应tagId
        val clusterTagMap: Map[Int, Long] = clusterIndexArray.map{case((_, clusterIndex), index) =>
            val tagId: Long = ruleMap(index.toString)
            // 返回 类簇中心点索引和属性标签ID
            (clusterIndex, tagId)
        }.toMap
        /*
          (0,364)
          (1,363)
          (2,362)
          (3,366)
          (4,365)
         */
        //clusterTagMap.foreach(println)

        // 4.3 自定义UDF函数，传递prediction，返回tagId
        val index_to_tag = udf(
            (clusterIndex: Int) => clusterTagMap(clusterIndex)
        )

        // 4.4 对预测数据，打标签
        val modelDF: DataFrame = predictionDF.select(
            $"uid", //
            index_to_tag($"prediction").as("tagId")
        )
        modelDF.printSchema()
        //modelDF.show(100, truncate = false)

        // 5. 返回标签模型数据
        modelDF
    }


    /**
     * 使用KMeans算法训练模型
     * @param dataframe 数据集
     * @return KMeansModel模型
     */
    def trainModel(dataframe: DataFrame): KMeansModel = {
        // i. 构建KMeans算法对象，设置相关参数
        val kMeans: KMeans = new KMeans()
            // 设置输入列和预测之列名称
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            .setK(5)
            .setMaxIter(20)
            .setInitMode("k-means||")
        // ii. 使用数据集训练模型
        val kMeansModel: KMeansModel = kMeans.fit(dataframe)
        // iii. 计算误差平方和SSE
        val wssse: Double = kMeansModel.computeCost(dataframe)
        println(s"WSSSE = $wssse")  //  WSSSE = 0.99785407724886
        // iv. 返回模型对象
        kMeansModel
    }
}

object RfmModel{
    def main(args: Array[String]): Unit = {
        val tagModel = new RfmModel()
        tagModel.executeModel(358L)
    }
}