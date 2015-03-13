package nasdaq_hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class SecondMapperReducer {
	static Logger log = Logger.getLogger(
			SecondMapperReducer.class.getName());

	public static class Map extends Mapper<LongWritable, Text, DoubleWritable,Text>{
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString();
			String[] val=line.split("\\s+");
			String stockName=val[1];
			String volatlity=val[0];
			Text companyInfo=new Text(stockName);
			DoubleWritable dwStockprice=new DoubleWritable(Double.parseDouble(volatlity));
			try {
				context.write(dwStockprice,companyInfo);
			} catch (Exception e) {
				System.out.println("error in mapper 2nd-mapper-reducer");
				log.info("SecondMapperReducer faced an issue while mapping+"+line+e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends Reducer<DoubleWritable, Text, Text, Text>{
		public static int count=0;
		public static HashMap<String,Double> map = new HashMap<String,Double>();
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val:values){
				if(count==0){					
					context.write(new Text("\nThe top 10 stocks with the lowest (min) volatility are"), new Text(""));
				}
				if(count<=9){
					context.write(val,new Text(key.toString()));
					map.put(val.toString(),key.get());
				}
				else{
					map.put(val.toString(),key.get());
				}
				count++;
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			int counter=0;
			context.write(new Text("\nThe top 10 stocks with the highest (max) volatility are"), new Text(""));
			ValueComparator bvc =  new ValueComparator(Reduce.map);
			TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc);
//			System.out.println("Size of the last map is "+sorted_map.size());
			sorted_map.putAll(Reduce.map);
			int i=0;
			for (Iterator<String> it=sorted_map.keySet().iterator();it.hasNext();){
				if(counter>=10){break;}

				String k=it.next();
				String val=""+map.get(k);
				context.write(new Text(k), new Text(val));
				counter++;
//				System.out.println("here");

			}
			String time=""+new Date().getTime();
			context.write(new Text(time), new Text(""));
		}
	}


	public static void action(String ... args) {
		try {
			// TODO check main settings before execution. 
			// Create a new Job
			Job job = Job.getInstance();
			job.getConfiguration().set("mapred.compress.map.output", "false");
			job.getConfiguration().setQuietMode(true);
			job.setJarByClass(SecondMapperReducer.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			/*	FileSystem hdfs =FileSystem.get(new Configuration());
			Path workingDir=hdfs.getWorkingDirectory();
			Path newFolderPath= new Path(args[1]);
			newFolderPath=Path.mergePaths(workingDir, newFolderPath);
			if(hdfs.exists(newFolderPath))
			{
			      hdfs.delete(newFolderPath, true); //Delete existing Directory
			}

//			hdfs.mkdirs(newFolderPath);*/
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			//			job.setJarByClass(FirstMapperReducer.class);
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
class ValueComparator implements Comparator<String> {

	HashMap<String, Double> base;
	public ValueComparator(HashMap<String, Double> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with equals.
	@Override
	public int compare(String a, String b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}
}