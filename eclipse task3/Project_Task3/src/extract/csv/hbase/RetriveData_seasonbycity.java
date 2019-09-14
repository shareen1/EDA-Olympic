package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_seasonbycity{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "SeasonByCity_olympic");
      FileWriter wc=new FileWriter("SeasonByCity_olympic.csv");
      wc.write("count,season,cityl\n");
      for(int i=1;i<44;i++) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(i+""));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("season"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("count"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("city"));

      // Printing the values
      String season = Bytes.toString(value).replace("\"", "");
      String count = Bytes.toString(value1);

      String city = Bytes.toString(value2).replace("\"", "");
      String out = count+","+season+","+city;
      wc.write(out+"\n");

     
      
      System.out.println(out);
      
      }
      wc.flush();
      wc.close();
   }
}