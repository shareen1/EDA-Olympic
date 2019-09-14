package extract.csv.hbase;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_GenderWise{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "GenderWiseOlympicMedalYearly");
      FileWriter wc=new FileWriter("GenderWiseOlympicMedalYearly.csv");
      wc.write("Year,count,sex\n");
      for(int i=1;i<70;i++) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(i+""));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("year"));

      byte [] value1 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("count"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sex"));

      // Printing the values
      String year = Bytes.toString(value);
      String count = Bytes.toString(value1);

      String sex = Bytes.toString(value2).replace("\"", "");
      String out = year +","+count+","+sex;
      wc.write(out+"\n");

     
      
      System.out.println("year: " + year +"Sex"+sex+"count"+count );
      
      }
      wc.flush();
      wc.close();
   }
}