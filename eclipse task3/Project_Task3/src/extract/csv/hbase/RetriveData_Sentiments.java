package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_Sentiments{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "SentimentVsMedalinUSA");
      FileWriter wc=new FileWriter("SentimentVsMedalinUSA.csv");
      wc.write("Year,Medals,Positive,negative\n");
      for(int i=2;i<=10;i=i+2) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes("row"+i));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("Year"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("Medals"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sentiment"));

      // Printing the values
      String year = Bytes.toString(value);
      String Medals = Bytes.toString(value1);
      String sentiment = Bytes.toString(value2).replace("\"", "");
      String out = year +","+Medals+","+sentiment;
      wc.write(out+"\n");

     
      
      System.out.println("year: " + year + " Medals: " + Medals+" sentiment :"+sentiment.replace("\"", ""));
      
      }
      wc.flush();
      wc.close();
   }
}