package vez.flinkstreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import vez.flinkstreaming.utils.KafkaOrdersDataGenerator;
import vez.flinkstreaming.utils.MariaDBManager;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This example is an example for Streaming Analytics in Flink.
 * It reads a real time orders stream from kafka, performs periodic summaries
 * and writes the output the a JDBC sink.
 ****************************************************************************/

public class StreamingAnalytics {

    public static void main(String[] args){

        try {

            System.out.println("******** Initiating Streaming Analytics *************");

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            // streamEnv.setParallelism(1);

            //This many number of connections will be open for MariaDB for updates
            System.out.println("Parallelism = " + streamEnv.getParallelism());

            //DB Tracker for printing summary every 5 sec from DB.
            MariaDBManager dbTracker = new MariaDBManager();
            dbTracker.setUp();
            Thread dbThread = new Thread(dbTracker);
            dbThread.start();

            //Initiate the Kafka Orders Generator
            KafkaOrdersDataGenerator ordersGenerator = new KafkaOrdersDataGenerator();
            Thread genThread = new Thread(ordersGenerator);
            genThread.start();

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.streaming.realtime");


            //Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>(
                    "streaming.orders.input", //topic
                    new JSONKeyValueDeserializationSchema(false),
                    properties //connection properties
            );

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest() ;

            //Create the data stream
            DataStream<ObjectNode> ordersRawInput = streamEnv.addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<SalesOrder> salesOrderObject = ordersRawInput
                    .map(new MapFunction<ObjectNode, SalesOrder>() {
                        @Override
                        public SalesOrder map(ObjectNode orderJson) {

                            System.out.println("--- Received Record : " + orderJson);
                            SalesOrder so = new SalesOrder();
                            so.setOrderId(orderJson.get("value").get("orderId").asInt());
                            so.setProduct(orderJson.get("value").get("product").asText());
                            so.setQuantity(orderJson.get("value").get("quantity").asInt());
                            so.setPrice(orderJson.get("value").get("price").asDouble());

                            return so;
                        }
                    });

            //Compute productwise 5 second total order value
            DataStream<Tuple3<String,String,Double>> productWindowedSummary =
                    salesOrderObject

                            //Extract Product and Total order value
                    .map(new MapFunction<SalesOrder,
                    Tuple2<String, Double>>() {
                             @Override
                             public Tuple2<String, Double> map(SalesOrder salesOrder)
                                     throws Exception {
                                 return new Tuple2<String,Double>(
                                         salesOrder.getProduct(),
                                         salesOrder.getQuantity()
                                                 * salesOrder.getPrice());
                             }
                         }
                    )

                            //Group by Product
                    .keyBy(new KeySelector<Tuple2<String, Double>, String>() {

                        @Override
                        public String getKey(Tuple2<String, Double> productValue)
                                throws Exception {
                            return productValue.f0;
                        }
                    })

                            //Create Tumbling window of 5 seconds
                    .window( TumblingProcessingTimeWindows.of(Time.seconds(5)))

                            //Compute Summary and publish results
                    .process(new ProcessWindowFunction<Tuple2<String, Double>,
                            Tuple3<String, String,Double>, String, TimeWindow>() {

                        @Override
                        public void process(String product,
                                            Context context,
                                            Iterable<Tuple2<String, Double>> iterable,
                                            Collector<Tuple3<String,String,Double>> collector)
                                throws Exception {

                            Double totalValue = 0.0;
                            for( Tuple2<String,Double> order : iterable ) {
                                totalValue = totalValue + order.f1;
                            }
                            collector.collect(new Tuple3<String,String,Double>(
                                    new Date(context.window().getStart()).toString(),
                                            product,
                                            totalValue));

                        }
                    });

            //Print Summary records
            productWindowedSummary
                    .map(new MapFunction<Tuple3<String, String, Double>, Object>() {
                        @Override
                        public Object map(Tuple3<String, String, Double> summary)
                                throws Exception {
                            System.out.println("Summary : "
                                    + " Window = " + summary.f0
                                    + ", Product = " + summary.f1
                                    + ", Total Value = " + summary.f2);
                            return null;
                        }
                    });

            productWindowedSummary
                        //Add sink to MySQL using the rich sink function
                    .addSink(new RichSinkFunction<Tuple3<String, String, Double>>() {
                        MariaDBManager dbUpdater;
                        //Open is called once per task slot
                        @Override
                        public void open(Configuration parameters) throws Exception {

                            super.open(parameters);
                            dbUpdater = new MariaDBManager();
                            dbUpdater.setUp();

                        }
                        //Called with connections need to be closed
                        @Override
                        public void close() throws Exception {
                            super.close();
                            dbUpdater.teardown();
                        }

                        //Called once per record.
                        @Override
                        public void invoke(Tuple3<String, String, Double> summary,
                                           Context context) throws Exception {

                            dbUpdater.insertSummary(
                                    summary.f0, summary.f1,summary.f2);
                        }
                    });

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Analytics");

            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
            }

    }
}
