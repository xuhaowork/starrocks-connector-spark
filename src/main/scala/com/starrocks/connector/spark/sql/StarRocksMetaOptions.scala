package com.starrocks.connector.spark.sql

object StarRocksMetaOptions {
  val DATA_TYPE_SIZE = "starrocks.datatype.size"
  val AGGREGATE_FUNCTION = "starrocks.aggregate.function"
  val PARTITION_PREDICATES = "starrocks.partition.predicates"
  val HASH_BUCKETS = "starrocks.ddl.hash.buckets"

  //  .withMetadata("starrocks.ddl.distributed_hash_buckets", 8)
//    .withMetadata("starrocks.datatype.size", 30),
//  StructField("c2", StringType)
//    .withMetadata("starrocks.ddl.is_duplicate_key", true)
//    .withMetadata("starrocks.ddl.is_aggregate_key", true)
//    .withMetadata("starrocks.ddl.is_unique_key", true)
//    .withMetadata("starrocks.ddl.is_primary_key", true)
//
//    .withMetadata("starrocks.column.size", 30)
//    .withMetadata("starrocks.partition.predicates", Array("1", "2"))

}
