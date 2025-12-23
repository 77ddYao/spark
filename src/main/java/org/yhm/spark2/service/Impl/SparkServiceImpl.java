package org.yhm.spark2.service.Impl;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import org.yhm.spark2.service.SparkService;

@Service
public class SparkServiceImpl implements SparkService {


    private static final Logger log = Logger.getLogger(SparkServiceImpl.class);

    @Override
    public Object queryData(String start, String end) {
        log.info("queryData start: " + start + " end: " + end);
        // 创建 SparkSession
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
                .appName("AisDataQuery")
                .master("local[*]")
                .getOrCreate();

        // 1. 读取目录下所有 CSV 文件
        // val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://lavm-yl28gyaaew:9001/data/ais/raw/2025/06/")
        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv("hdfs://lavm-yl28gyaaew:9001/data/ais/raw/2025/06/");
        // 2. 过滤数据 筛选出 BaseDateTime 在 2025-06-26 00:00:00 到 2025-06-26 00:00:10 的所有记录，然后打印这些记录
        Dataset<Row> filteredDf = df.filter(df.col("base_date_time").between(start, end));
        filteredDf.show();

        // 将DataFrame转换为JSON字符串数组，便于Web API返回
        String[] jsonRows = filteredDf.toJSON().collectAsList().toArray(new String[0]);
        log.info("queryData jsonRows: " + jsonRows);

        spark.close(); // 关闭SparkSession以释放资源
        return jsonRows;
    }
}
