package extract.csv.hbase;
import java.awt.List;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData_TopTen{

   public static void main(String[] args) throws IOException, Exception{
	   String[] row = new String[]{"CAN", "FRA","GBR","GER","HUN","ITA","RUS","SWE","URS","USA"};                                                                                                                                          
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "TopTenContryByGoldMedal");
      FileWriter wc=new FileWriter("TopTenContryByGoldMedal.csv");
      wc.write("NOC,NumberOfGoldMedal\n");
      for(String r :row) 
      {
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(r));

      // Reading the data
      Result result = table.get(g);


      // Reading values from Result class object
      byte [] values = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes(""));

    
      // Printing the values
      String Medal = Bytes.toString(values);
      String NOC = r;
      String out=NOC+","+Medal;
      wc.write(out+"\n");

     
      
      System.out.println("NOC: " + NOC + " No Medals: " + Medal );
      }
      wc.flush();
      wc.close();
   }
}