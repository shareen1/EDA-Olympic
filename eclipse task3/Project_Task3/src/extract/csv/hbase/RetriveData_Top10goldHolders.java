package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_Top10goldHolders{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "Top10Goldholders_olympic");
      FileWriter wc=new FileWriter("Top10Goldholders_olympic.csv");
      wc.write("count,name\n");
      for(int i=1;i<11;i++) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(i+""));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("name"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("count"));
      
      

      // Printing the values
      String name = Bytes.toString(value).replace("\"", "");
      String count = Bytes.toString(value1);

  
      String out = count+","+name;
      wc.write(out+"\n");

     
      
      System.out.println(count+","+name);
      
      }
      wc.flush();
      wc.close();
   }
}