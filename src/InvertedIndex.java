import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.*;


public class InvertedIndex {
	public static void main(String args[]) 
		throws Exception {
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true)? 0:1);
	}

	static class Map extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String DocID = ((FileSplit) context.getInputSplit()).getPath().getName();
			String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);
      
      StringTokenizer tokenizer = new StringTokenizer(value_raw, " '-");
      
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());

				context.write(word, new Text(DocID));
			}
		}
	}

	static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {			
			HashMap<String, Integer> m = new HashMap<String, Integer>();
			
			
			for (Text t: values) {
				if(m.putIfAbsent(t.toString(), 1) != null) {
					m.put(t.toString(), m.get(t.toString()) + 1);
				}
			}
			
			StringBuilder mapString = new StringBuilder();
			for (String val : m.keySet()) {
				mapString.append(val);
				mapString.append(":");
				mapString.append(m.get(val));
				mapString.append(" ");
			}
			//Writes the HashMap
			context.write(key, new Text(mapString.toString()));
		}
	}
}