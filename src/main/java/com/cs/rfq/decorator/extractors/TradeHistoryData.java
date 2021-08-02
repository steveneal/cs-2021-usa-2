package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.function.ForeachFunction;

public class TradeHistoryData implements RfqMetadataExtractor {



    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT SecurityID, TradeDate, LastQty, LastPx from trade where TraderId='%s'",
                rfq.getTraderId());

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);


       String SecId ="SecurityId " + sqlQueryResults.first().get(0);
       String TraDa ="TradeDate " + sqlQueryResults.first().get(1);
       String LQty ="LastQty " + sqlQueryResults.first().get(2);
        String LPx ="LastPx " + sqlQueryResults.first().get(3);
        Object volume = SecId + " "+ TraDa+ " "+ LQty+ " "+ LPx + "/n";
        //Object volume = sqlQueryResults.head(10);

        if (volume == null) {
            volume = 0L;
        }
        System.out.println(1);
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradeHistoryData, volume);
        return results;
    }



    public static void main(String[] args) {

    }
}

