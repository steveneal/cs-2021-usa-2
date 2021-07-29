package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityYTDExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class dev {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local");

        SparkSession session = SparkSession.builder()
                .appName("Hello Spark")
                .getOrCreate();

        String pth = "src\\test\\resources\\trades\\trades.json";
        TradeDataLoader lder = new TradeDataLoader();
        Dataset<Row> trades = lder.loadTrades(session, pth);
//        System.out.println(trades);

//        Rfq requests = new Rfq();

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq requests = Rfq.fromJson(validRfqJson);

        VolumeTradedWithEntityYTDExtractor volExtractor = new VolumeTradedWithEntityYTDExtractor();
        Map<RfqMetadataFieldNames, Object>  volMap = volExtractor.extractMetaData(requests, session, trades);
        System.out.println(requests.getIsin() + ": " + volMap);

    }
}
