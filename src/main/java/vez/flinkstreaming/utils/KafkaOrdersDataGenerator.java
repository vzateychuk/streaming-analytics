package vez.flinkstreaming.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import vez.flinkstreaming.SalesOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/****************************************************************************
 * This Generator generates a a series of Orders into Kafka at random
 * intervals. This can be used to test Streaming Analytics pipelines
 ****************************************************************************/

public class KafkaOrdersDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    //Kafka topic to publish to
    public static final String topic = "streaming.orders.input";

    public static void main(String[] args) {
        KafkaOrdersDataGenerator kodg = new KafkaOrdersDataGenerator();
        kodg.run();
    }

    public void run() {

        try {

            System.out.println("Starting Kafka Orders Generator..");
            //Wait for the main flow to be setup.
            Thread.sleep(5000);

            //Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers","localhost:9092");

            kafkaProps.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String,String> myProducer
                    = new KafkaProducer<String, String>(kafkaProps);

            //Define list of Products
            List<String> products = new ArrayList<String>();
            products.add("Keyboard");
            products.add("Mouse");
            products.add("Monitor");

            //Define list of Prices. Matches the corresponding products
            List<Double> prices = new ArrayList<Double>();
            prices.add(25.00);
            prices.add(10.5);
            prices.add(140.00);

            //Define a random number generator
            Random random = new Random();

            ObjectMapper mapper = new ObjectMapper();

            //Capture current timestamp
            String currentTime = String.valueOf(System.currentTimeMillis());

            //Create order ID based on the timestamp
            int orderId = (int)Math.floor(System.currentTimeMillis()/1000);

            //Generate 100 sample order records
            for(int i=0; i < 100; i++) {

                SalesOrder so = new SalesOrder();
                so.setOrderId(orderId);
                orderId++;

                //Generate a random product
                int randval=random.nextInt(products.size());
                so.setProduct(products.get(randval));

                //Get product price
                so.setPrice(prices.get(randval));

                //Generate a random value for number of quantity
                so.setQuantity(random.nextInt(4) + 1);

                String recKey = String.valueOf(so.getOrderId());
                String value = mapper.writeValueAsString(so);

                //Create a Kafka producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                topic,
                                recKey,
                                value );

                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(ANSI_PURPLE +
                            "Kafka Orders Stream Generator : Sending Event : "
                            + String.join(",", value)  + ANSI_RESET);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
