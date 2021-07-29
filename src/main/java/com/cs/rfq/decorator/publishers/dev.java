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
        // Initial Spark Setup taken from the class Hello Spark file
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local");

        SparkSession session = SparkSession.builder()
                .appName("Hello Spark")
                .getOrCreate();

        // Create our 'Historical Trades Database' based on the previously created .json
        String pth = "src\\test\\resources\\trades\\trades.json";
        TradeDataLoader lder = new TradeDataLoader();
        Dataset<Row> trades = lder.loadTrades(session, pth);

        // Create a Sample Request coming in as String
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        // Parse String request
        Rfq requests = Rfq.fromJson(validRfqJson);

        // Create Volume extractor class and run the method to obtain the key value pair
        // TODO: Call the other aggregator functions here
        VolumeTradedWithEntityYTDExtractor volExtractor = new VolumeTradedWithEntityYTDExtractor();

        // The method return a key value pair, <volumeTradedYearToDate, volume>
        Map<RfqMetadataFieldNames, Object>  volMap = volExtractor.extractMetaData(requests, session, trades);

        // We will need to save this value as a field in our requests or save somewhere as "metadata"
        System.out.println(requests.getIsin() + ": " + volMap);

    }
}
