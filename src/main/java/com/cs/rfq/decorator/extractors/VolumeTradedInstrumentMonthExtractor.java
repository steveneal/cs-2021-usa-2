package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedInstrumentMonthExtractor implements RfqMetadataExtractor{

        private String since;

    public VolumeTradedInstrumentMonthExtractor() {
        if (DateTime.now().getMonthOfYear() < 10){
            this.since = DateTime.now().getYear() +"-0"+DateTime.now().getMonthOfYear()+ "-01";
        }else{
            this.since = DateTime.now().getYear() + DateTime.now().getMonthOfYear()+ "-01"; }

    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query = String.format("SELECT SUM(LastQty) from trade where TraderId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedSecMonthToDate, volume);
        return results;
    }
    protected void setSince(String since) {
        this.since = since;
    }
}

