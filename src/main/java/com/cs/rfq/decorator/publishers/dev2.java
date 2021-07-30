package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.extractors.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class dev2 {
    public static void main(String[] args) {
        // Initial Spark Setup taken from the class Hello Spark file
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local");

        SparkSession session = SparkSession.builder()
                .appName("Hello Spark")
                .getOrCreate();

        // Create our 'Historical Trades Database' based on the previously created .json
        String pth = "src\\test\\resources\\trades\\trades.json";
        Dataset<Row> trades = session.read().json(pth);

        trades.show();

        trades.select(col("OrderID"),col("LastQty").divide(1000)).show();


    }
}
