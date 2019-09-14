import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Task3Mapper extends Mapper<Object, Text, Text, NullWritable>{
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] arrLine = value.toString().split(",");
		if(arrLine.length==13)
		{
			String noc=arrLine[6].replace("\"", "");
			String year=arrLine[7].replace("\"", "");
			String medal=arrLine[11].replace("\"", "");
			String uniqueId=arrLine[12].replace("\"", "");
			if (noc.equalsIgnoreCase("USA")  && !medal.equalsIgnoreCase("participant") ) 
			{
			
			//System.out.println("NOC :"+noc.replace("\"", "")+"Year"+year+"Meadl"+medal);
			context.write(new Text("NOTSERVEY,"+noc+","+year+","+medal+","+uniqueId),NullWritable.get() );
			}
			
		}else if(arrLine.length==3)
		{
			String year=arrLine[0];
			String positive=arrLine[1];
			String negetive=arrLine[2];
			context.write(new Text("SERVEY,"+year+","+positive+","+negetive),NullWritable.get() );
		}
		//System.out.println(arrLine.length);
		
		
	}

}
