package com.zxftech.rrms;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MyMain {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2) {
            throw new RuntimeException("Data date and partition path were expected !");
        }
        String datadate = args[0];
        String partition_path = args[1];
        System.out.println("===========================client mode==================================");
        SparkSession spark = SparkSession.builder().
                master("spark://vm156:7077").// 进在开发测试时使用，正式上线提交时，可以不写，因为在提交命令
                appName("spark program demo").
                enableHiveSupport().
                getOrCreate();
        dataSetDemo(spark, datadate, partition_path);
    }

    public static void dataSetDemo(SparkSession spark, String datadate, String partition_path) {

        spark.sql("alter table ldh.custinfo add if not exists partition (datadate='" + datadate + "') location '" + partition_path + "';");

        // custinfo表结构为:cstnum name address gender datadate
        Dataset<Row> custinfo = spark.table("ldh.custinfo");

        // 筛选出 日期大于20191220的记录的cstnum、name两个字段，存储到临时表t_result中
        custinfo.filter(new Column("datadate").endsWith(datadate)).select(new Column("cstnum"), new Column("name")).registerTempTable("t_result");

        // 存储到result表中
        spark.sql("insert into table ldh.result select * from t_result");
    }
}
