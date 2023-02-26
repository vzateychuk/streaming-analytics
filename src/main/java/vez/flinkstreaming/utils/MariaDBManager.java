package vez.flinkstreaming.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/****************************************************************************
 * This Class creates and manages connections to MariaDB with an
 * order_summary table. Its run() method prints the summary of the orders table at
 * periodic intervals. It also supports an insert function to insert
 * new records to the table for testing purposes
 ***************************************************************************
 **/

public class MariaDBManager implements Runnable, Serializable {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    private Connection conn;

    public static void main(String[] args) {

        System.out.println("Starting MariaDB DB Manager");
        MariaDBManager sqldbm = new MariaDBManager();
        sqldbm.setUp();
        sqldbm.insertSummary("2020-08-20 00:00:00","Mouse",46.00);
        sqldbm.run();
    }

    public void setUp() {
        System.out.println("Setting up MariaDB Connection");
        String url = "jdbc:mysql://localhost:3306/streaming";
        try {
            conn = DriverManager.getConnection(url,"streaming","streaming");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void teardown() {
        try {
            conn.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //Used by StreamingAnalytics for writing data into MariaDB
    public void insertSummary(String timestamp, String product, Double value) {
        try{

            String sql = "INSERT INTO order_summary "
                            + "(INTERVAL_TIMESTAMP, PRODUCT, TOTAL_VALUE) VALUES "
                            +"( '" + timestamp + "',"
                            +" '" + product + "',"
                            + value + ")";
            System.out.println(sql);
            conn.createStatement().execute(sql);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //Use to print 5 second statistics from the MariaDB Database
    @Override
    public void run() {

        try {

            //Find latest ID
            int latestId=0;

            String idQuery="SELECT IFNULL(MAX(ID),0) as LATEST_ID "
                                + "FROM order_summary";
            ResultSet rsLatest = conn.createStatement().executeQuery(idQuery);

            while(rsLatest.next()) {
                latestId = rsLatest.getInt("LATEST_ID");
            }


            //SQL for periodic stats
            String selectSql = "SELECT count(*) as TotalRecords, "
                                + " sum(TOTAL_VALUE) as TotalValue"
                                + " FROM order_summary"
                                + " WHERE ID > " + latestId ;
            System.out.println("Periodic Check Query : " + selectSql);

            while(true) {
                //Sleep for 5 seconds and then query summary
                Thread.sleep(5000);
                ResultSet rs = conn.createStatement().executeQuery(selectSql);

                while (rs.next()) {
                    System.out.println(ANSI_BLUE
                            +"---------------------------------------------------------------\n "
                            +"DB Summary : "
                            + "Records = " + rs.getInt("TotalRecords") + ", "
                            + "Value = " + rs.getDouble("TotalValue") + "\n"
                            +"---------------------------------------------------------------"
                            + ANSI_RESET);
                }
            }



        } catch(Exception e) {
            e.printStackTrace();
        }

    }
}
