import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	Map<String, String> serveymap = new HashedMap();
	Map<String, Integer> nonserveymap = new HashedMap();
	int j=0;
	
	
	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		String[] arrLine = key.toString().split(",");
		if (arrLine[0].equals("SERVEY")) {
			String value = arrLine[2] + "," + arrLine[3];
			serveymap.put(arrLine[1].replace("\"", ""), value);
		} else {
			// String value = arrLine[1]+","+arrLine[2];
			int int_year=Integer.parseInt(arrLine[2]);
			if (int_year >= 2000 && (int_year != 2006) && (int_year != 2002) && (int_year != 2010)
					&& (int_year != 2014)) {
				Integer i = nonserveymap.get(arrLine[2]);

				if (null == i) {
					i = 0;
				}
				nonserveymap.put(arrLine[2], i + 1);
			}
		}

	}

	@Override
	protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		int j=0; Set<Entry<String, Integer>> set =nonserveymap.entrySet();
		  Iterator<Entry<String, Integer>> i = set.iterator(); 
		  while (i.hasNext()) { 
			  j++;
			  Entry me = (Map.Entry) i.next();
			  
			  String sentimentval = serveymap.get(me.getKey());
			  if(null==sentimentval) {
				  sentimentval ="unknown,unknown";
			  }
			  Text outkey = new Text( me.getKey()+","+me.getValue()+","+sentimentval ); 
			  j=j+1;
			  InsertData.insert(me.getKey().toString(),me.getValue().toString(),sentimentval,j);
			  context.write(outkey, NullWritable.get()); } 
		  super.cleanup(context);
	}

}
