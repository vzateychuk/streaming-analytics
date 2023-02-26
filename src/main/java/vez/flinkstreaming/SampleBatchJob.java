package vez.flinkstreaming;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SampleBatchJob {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<String> list = Arrays.asList("Mouse", "Keyb", "Webcam");

    DataSet<String> products = env.fromCollection(list);

    System.out.println("---> Products count: " + products.count());
    //
  }
}
