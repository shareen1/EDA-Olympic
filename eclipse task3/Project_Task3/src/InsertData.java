import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData{

	public static void insert(String  key, String  value, String sentimentval,int i) {
	
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable hTable = null;
	try {
		hTable = new HTable(config, "SentimentVsMedalinUSA");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

      // Instantiating Put class
      // accepts a row name.
      Put p = new Put(Bytes.toBytes("row"+i)); 

      // adding values using add() method
      // accepts column family name, qualifier/row name ,value
      p.add(Bytes.toBytes("cf"),
      Bytes.toBytes("Year"),Bytes.toBytes(key));

      p.add(Bytes.toBytes("cf"),
    	      Bytes.toBytes("Medals"),Bytes.toBytes(value));
      p.add(Bytes.toBytes("cf"),
    	      Bytes.toBytes("sentiment"),Bytes.toBytes(sentimentval));


//      p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
//      Bytes.toBytes("manager"));
//
//      p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
//      Bytes.toBytes("50000"));
      
      // Saving the put Instance to the HTable.
      try {
		hTable.put(p);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      System.out.println("data inserted");
      
      // closing HTable
      try {
		hTable.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }


}