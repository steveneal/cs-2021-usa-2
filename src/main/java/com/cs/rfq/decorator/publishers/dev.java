package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.extractors.*;
import org.apache.commons.lang.time.DateUtils;

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
        // TODO: Call the other aggregator functions for Year here

        VolumeTradedEntityYearExtractor volExtractor = new VolumeTradedEntityYearExtractor();
        VolumeTradedEntityMonthExtractor volMonthExtractor = new VolumeTradedEntityMonthExtractor();
        VolumeTradedEntityWeekExtractor volWeekExtractor = new VolumeTradedEntityWeekExtractor();
        VolumeTradedInstrumentMonthExtractor volMonthSecExtractor = new VolumeTradedInstrumentMonthExtractor();
        VolumeTradedInstrumentYearExtractor volYearSecExtractor = new VolumeTradedInstrumentYearExtractor();
        VolumeTradedInstrumentWeekExtractor volWeekSecExtractor = new VolumeTradedInstrumentWeekExtractor();
        AverageTradedPriceExtractor avetradeExtractor = new AverageTradedPriceExtractor();
        InstrumentLiquidityExtractor instrumentliquidExtractor = new InstrumentLiquidityExtractor();

        // The method return a key value pair, <volumeTradedYearToDate, volume>
        Map<RfqMetadataFieldNames, Object>  volMap = volExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volMonthMap = volMonthExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volWeekMap = volWeekExtractor.extractMetaData(requests, session, trades);

        Map<RfqMetadataFieldNames, Object>  volMonthSecMap = volMonthSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volWeekSecMap = volWeekSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volYearSecMap = volYearSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  avetradeMap = avetradeExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  liquidMap = instrumentliquidExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  tradeBias = tradeSideBias.extractMetaData(requests, session, trades);


        // We will need to save this value as a field in our requests or save somewhere as "metadata"
        System.out.println(requests.getIsin() + ": " + volMap);
        System.out.println(requests.getIsin() + ": " + volMonthMap);
        System.out.println(requests.getIsin() + ": " + volWeekMap);
        System.out.println(requests.getIsin() + ": " + volMonthSecMap);
        System.out.println(requests.getIsin() + ": " + volWeekSecMap);
        System.out.println(requests.getIsin() + ": " + volYearSecMap);
        System.out.println(requests.getIsin() + ": " + avetradeMap);
        System.out.println(requests.getIsin() + ": " + liquidMap);
        System.out.println(requests.getEntityId()+ ":  " +tradeBias);


    }
}
