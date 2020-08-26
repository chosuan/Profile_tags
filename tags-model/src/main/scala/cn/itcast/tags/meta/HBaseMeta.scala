package cn.itcast.tags.meta

/**
 * HBase 元数据解析存储，具体数据字段格式如下所示：
 * inType=hbase
 * zkHosts=bigdata-cdh01.itcast.cn
 * zkPort=2181
 * 依据inType类型解析为HBaseMeta，加载业务数据，核心代码如下：
 * hbaseTable=tbl_tag_users
 * family=detail
 * selectFieldNames=id,gender
 */
case class HBaseMeta(zkHosts: String,
                     zkPort: String,
                     hbaseTable: String,
                     family: String,
                     selectFieldNames: String)

object HBaseMeta {
    /**
     * 将Map集合数据解析到HBaseMeta中
     *
     * @param ruleMap map集合
     * @return
     */
    def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
        // TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
        HBaseMeta(
            ruleMap("zkHosts"),
            ruleMap("zkPort"),
            ruleMap("hbaseTable"),
            ruleMap("family"),
            ruleMap("selectFieldNames")
        )
    }
}
