package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    private java.sql.Date Month;
    private java.sql.Date Week;
    private long sellSide = 2;
    private long buySide = 1;
    private int ratioWeek;
    private int ratioMonth;

    public TradeSideBiasExtractor() {
        long weekly = new DateTime().minusWeeks(1).getMillis();
        long monthly = new DateTime().minusMonths(1).getMillis();
        this.Week = new java.sql.Date(weekly);
        this.Month = new java.sql.Date(monthly);
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        //Weekly buy and sell query
        String weekBuySide = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 1",
                rfq.getEntityId(),
                rfq.getIsin(),
                Week);
        Object WeeklyBuySideVolume = session.sql(weekBuySide).first().get(0);
        String weekSellSide = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 2",
                rfq.getEntityId(),
                rfq.getIsin(),
                Week);
        Object SellSideWeekVolume = session.sql(weekSellSide).first().get(0);

        //Monthly buy and sell query
        String buySideMonth = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 1",
                rfq.getEntityId(),
                rfq.getIsin(),
                Month);
        Object BuySideMonthVolume = session.sql(buySideMonth).first().get(0);
        String sellSideMonth = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 2",
                rfq.getEntityId(),
                rfq.getIsin(),
                Month);
       trades.createOrReplaceTempView("trade");
        Object SellSideMonthVolume = session.sql(sellSideMonth).first().getLong(0);

        Map<RfqMetadataFieldNames, Object> result = new HashMap<>();
        //report the ratio (as a single figure) of buy/sell traded for downstream systems to display.
        //result will be negative if no trade being made in instrument and add to map, else divide the volume and add to Map
        if (WeeklyBuySideVolume == null || SellSideWeekVolume == null){
            ratioWeek= -1;
            result.put(RfqMetadataFieldNames.tradeSideBiasWeek, ratioWeek);
        }else{
            ratioWeek = (int)((Long) WeeklyBuySideVolume *100.0/ (Long)SellSideWeekVolume+0.5);
            result.put(RfqMetadataFieldNames.tradeSideBiasWeek, ratioWeek);
        }

        if (BuySideMonthVolume == null || SellSideMonthVolume == null){
            ratioMonth = -1;
            result.put(RfqMetadataFieldNames.tradeSideBiasMonth, ratioMonth);
        }else{
            ratioMonth = (int)((long) BuySideMonthVolume *100.0/ (long)SellSideMonthVolume+.05);
            result.put(RfqMetadataFieldNames.tradeSideBiasMonth, ratioMonth);
        }
        System.out.println("Monthly Ratio:"+ratioMonth);
        System.out.println("Monthly Buy Volume "+ BuySideMonthVolume);
        System.out.println("Monthly Sell Volume  "+ SellSideMonthVolume);
        return result;
    }
    protected void setWeekSince(java.sql.Date week) {this.Week = week; }
    protected void setMonthSince(java.sql.Date month) {this.Month = month; }
}
/*
    //Filtering Weekly Date
    public static Dataset<Row> weekFilter(Dataset<Row> dataset, String col) {
        long today = new DateTime().withMillisOfDay(0).getMillis();
        long week = new DateTime().withMillis(today).minusWeeks(1).getMillis();
        return dataset.filter(dataset.col(col).$greater(new java.sql.Date(week)));
    }

public static Dataset<Row> monthFilter(Dataset<Row> dataset, String col) {
    long today = new DateTime().withMillisOfDay(0).getMillis();
    long month = new DateTime().withMillis(today).minusMonths(1).getMillis();
    return dataset.filter(dataset.col(col).$greater(new java.sql.Date(month)));
}
    */