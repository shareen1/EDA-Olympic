package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_AVGheightVsweight{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "AVGheightVsWeight_olympic");
      FileWriter wc=new FileWriter("AVGheightVsWeight_olympic.csv");
      wc.write("avg_height,avg_weight,sport\n");
      for(int i=1;i<68;i++) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(i+""));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("height"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("weight"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sport"));

      // Printing the values
      String height = Bytes.toString(value);
      String weight = Bytes.toString(value1);

      String sport = Bytes.toString(value2).replace("\"", "");
      String out = height+","+weight+","+sport;
      wc.write(out+"\n");

     
      
      System.out.println(height+","+weight+","+sport);
      
      }
      wc.flush();
      wc.close();
   }
}