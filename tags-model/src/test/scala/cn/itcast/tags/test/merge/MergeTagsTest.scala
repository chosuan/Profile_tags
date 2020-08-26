package cn.itcast.tags.test.merge

object MergeTagsTest {

    def main(args: Array[String]): Unit = {

        // 画像标签
        val tagIds: String = "319,322,329,333,354"
        // 计算标签
        val tagId: String = "348"
        // 消费周期所有标签
        val ids: Set[String] = Set("348", "349", "350", "351", "352", "353", "354", "355")

        // 画像标签Set集合
        val tagIdsSet: Set[String] = tagIds.split(",").toSet
        //交集
        val insertSet: Set[String] = tagIdsSet & ids
        //合并后新标签
        val newTagIds: Set[String] = tagIdsSet -- insertSet + tagId

        println(newTagIds.mkString(","))

    }

}
