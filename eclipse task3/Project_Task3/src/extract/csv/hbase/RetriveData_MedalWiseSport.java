package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_MedalWiseSport{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "MedalWiseSport_olympic");
      FileWriter wc=new FileWriter("MedalWiseSportolympic.csv");
      wc.write("count,sport,medal\n");
      for(int i=1;i<250;i++) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(i+""));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sport"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("count"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("medal"));

      // Printing the values
      String sport = Bytes.toString(value).replace("\"", "");
      String count = Bytes.toString(value1);

      String medal = Bytes.toString(value2).replace("\"", "");
      String out = count+","+sport+","+medal;
      wc.write(out+"\n");

     
      
      System.out.println("count"+count+"sport,"+sport+"medal,"+medal);
      
      }
      wc.flush();
      wc.close();
   }
}