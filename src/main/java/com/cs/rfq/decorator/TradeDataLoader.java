package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Currency;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    // Idea1 :
    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema = new StructType()
                .add("TraderId", LongType, true)
                .add("EntityId", LongType, true)
                .add("MsgType", StringType, true)
                .add("TradeReportId", LongType, true)
                .add("PreviouslyReported", StringType, true)
                .add("SecurityID", StringType, true)
                .add("SecurityIdSource", StringType, true)
                .add("LastQty", LongType, true)
                .add("TradeDate", DateType, true)
                .add("LastPx", DoubleType, true)
                .add("TransactTime", StringType, true)
                .add("NoSides", StringType, true)
                .add("Sides", StringType, true)
                .add("OrderID", LongType, true)
                .add("Currency", StringType, true);
        System.out.println(schema);

        //TODO: load the trades dataset
        // After defining the schema we need to read the json file with the schema
        Dataset<Row> trades = session.read().schema(schema).json(path);
//        Dataset<Row> trades = session.read().json(path);
        //TODO: log a message indicating number of records loaded and the schema used
        System.out.println(trades.count());  // jank setup need to figureout logging later
        return trades;
    }

}
