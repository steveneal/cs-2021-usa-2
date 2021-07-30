package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.SQLOutput;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedEntityWeekExtractor implements RfqMetadataExtractor{


    private String since;
    Date yourDate = DateUtils.addDays(new Date(), -7);
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String strDate = dateFormat.format(yourDate);

    public VolumeTradedEntityWeekExtractor() {
        this.since = strDate;
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),

                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradesWithEntityPastWeek, volume);
        return results;
    }
    protected void setSince(String since) {
        this.since = since;
    }
}