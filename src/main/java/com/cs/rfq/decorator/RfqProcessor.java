package com.cs.rfq.decorator;


import com.cs.rfq.decorator.extractors.*;

//import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
//import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
//import com.cs.rfq.decorator.extractors.TotalTradesWithEntityExtractor;
//import com.cs.rfq.decorator.extractors.VolumeTradedEntityYearExtractor;
//import com.cs.rfq.decorator.extractors.VolumeTradedEntityWeekExtractor;

import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    static void consume(Rfq line){System.out.println(line);}

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader dataLoader = new TradeDataLoader();
        trades = dataLoader.loadTrades(session, "src/test/resources/trades/trades.json");

        //TradeSideBiasExtractor tradeSideBias = new TradeSideBiasExtractor();

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new AverageTradedPriceExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new TradeSideBiasExtractor());
        extractors.add(new VolumeTradedEntityWeekExtractor());
        extractors.add(new VolumeTradedEntityMonthExtractor());
        extractors.add(new VolumeTradedEntityYearExtractor());
        extractors.add(new VolumeTradedInstrumentWeekExtractor());
        extractors.add(new VolumeTradedInstrumentMonthExtractor());
        extractors.add(new VolumeTradedInstrumentYearExtractor());




    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        JavaDStream<Rfq> words = lines.flatMap(x -> {
                Rfq asRfq = Rfq.fromJson(x);
                return Arrays.asList(asRfq).iterator();
            });

        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> {processRfq(line); consume(line);});
        });

        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        // Idea: Because every extractor class has the same method to create their metadata
        //       We loop through the list of extractors and call each extractor's method
        for(RfqMetadataExtractor extractor: extractors){
            metadata.putAll(extractor.extractMetaData(rfq, session, trades));
        }

        //TODO: publish the metadata
        // Idea:
        publisher.publishMetadata(metadata);
        //System.out.println(metadata);
    }
}
