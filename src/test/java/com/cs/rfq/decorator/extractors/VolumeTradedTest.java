package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.volumeTradedYearToDate;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VolumeTradedTest extends AbstractSparkUnitTest {

    Dataset<Row> trades;


    @Test
    public void loadTradeRecords() {
        assertEquals(5, trades.count());
    }

    @Test
    public void loadTradeFields() {
                //2018-06-09
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
        Map<RfqMetadataFieldNames, Object> volMap = volExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volMonthMap = volMonthExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volWeekMap = volWeekExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volMonthSecMap = volMonthSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volWeekSecMap = volWeekSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  volYearSecMap = volYearSecExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  avetradeMap = avetradeExtractor.extractMetaData(requests, session, trades);
        Map<RfqMetadataFieldNames, Object>  liquidMap = instrumentliquidExtractor.extractMetaData(requests, session, trades);

        int volMapint = volMap.get(volumeTradedYearToDate);
        assertAll(
                () -> assertEquals("{volumeTradedYearToDate=2388450000}", volMap),
                () -> assertEquals((Long) 5561279226039690843L, volMonthMap),
                () -> assertEquals((String) "35", volWeekMap),
                () -> assertEquals(7501009173671742065L, volMonthSecMap),
                () -> assertEquals("N", volWeekSecMap),
                () -> assertEquals("AT0000A0VRQ6", volYearSecMap),
                () -> assertEquals("4", avetradeMap),
                () -> assertEquals((Long) 500000L, liquidMap)
        );
    }
}
