package nasdaq_hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
public class FirstMapperReducer {
	static Logger log = Logger.getLogger(
			FirstMapperReducer.class.getName());
	private static String prevName;
	private static String prevYr;
	private static String prevMon;
	private static String prevPrice;
	private static String prevDay;

	//  Format for the CSV being parsed
	//			Date,Open,High,Low,Close,Volume,Adj Close
	//			2014-12-30,32.86,32.95,32.86,32.95,1200,32.95
	//			 we need date, adj close i.e. [0] and [6]

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private String price;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] companyInfo = line.split(",");
			Text key1;
			Text value1;
			if(companyInfo.length!=7){
				// There was issue cause the input was not in expected format.
				log.info("The CompanyInfo array had less than 7 elements");
			}else if(companyInfo[0].equals("Date")){
				log.info("Just encountered the Desc. line");
				//Initialize the prev enteries
			}
			else{
				String dateInfo[]=companyInfo[0].split("-");
				// We need year and month for key
				if(dateInfo.length!=3){
					log.info("The date did not have the required parts");
				}else{
					// We got year as [0] and month as [1] and day as [2]
					//Now we need filename that we are dealing with
					String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
					fileName=fileName.substring(0,fileName.length() - 4);

					String[] checkDash=fileName.split("-");
					if(checkDash.length>0){
						fileName=checkDash[0];
					}
					else if(fileName.lastIndexOf(".")!=-1){						
						fileName = fileName.substring(0, fileName.lastIndexOf("."));
					}
					price=companyInfo[6];


					if(prevName==null){
						// this is the first day of the month
						prevName=fileName;
						prevPrice=companyInfo[6];
						prevMon=dateInfo[1];
						prevYr=dateInfo[0];
						prevDay=dateInfo[2];
						// also out the info as first day of month is required.
						key1= new Text(fileName);
						value1=new Text(companyInfo[0]+"-"+companyInfo[6]);
						context.write(key1, value1);
					}
					else {
						//						Need to compare the value with previous month
						if(fileName.equals(prevName)){
							// same stock
							if(dateInfo[0].equals(prevYr)&& dateInfo[1].equals(prevMon)){
								//month and year match.
								prevPrice=companyInfo[6];
								prevDay=dateInfo[2];
							}
							else{
								// the month or the yr changed
								// record the last value of the month

								key1=new Text(prevName);
								value1=new Text(prevYr+"-"+prevMon+"-"+prevDay+"-"+prevPrice);
								context.write(key1, value1);


								//record value for the first day of this month
								key1= new Text(fileName);
								value1=new Text(companyInfo[0]+"-"+companyInfo[6]);
								context.write(key1, value1);
								// and also change the prev month and date value

								prevName=fileName;
								prevPrice=companyInfo[6];
								prevMon=dateInfo[1];
								prevYr=dateInfo[0];
								prevDay=dateInfo[2];
							}
						}

					}

					key1= new Text(fileName);
					value1=new Text(companyInfo[0]+"-"+companyInfo[6]);
					try {
						context.write(key1, value1);
					} catch (Exception e) {
						e.printStackTrace();
						log.error("Encountered an exception"+e.toString());
					}

				}

			}


		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// printing for the last record
			if(prevName!=null && !prevName.isEmpty()){
				context.write(new Text(prevName),new Text(prevYr+"-"+prevMon+"-"+prevDay+"-"+prevPrice));
			}
		}			
	}


	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String,TreeMap> mySet= new HashMap<String,TreeMap>();
			TreeMap<Integer,String> myTree =new TreeMap<Integer,String>();
			ArrayList<Double> monthVal= new ArrayList<Double>();

			//Value we get is in form of STOCK_NAME, YR-MON-DAY-PRICE
			//map key is stock_name+yr+day and value will be a set.
			for(Text val :values){
				String stockInfo[]=val.toString().split("-");
				String mapKey=key+"-"+stockInfo[0]+"-"+stockInfo[1];
				int day=Integer.parseInt(stockInfo[2]);
				String stockPrice=stockInfo[3];

				if(mySet.containsKey(mapKey)){
					myTree=mySet.get(mapKey);
					myTree.put(day, stockPrice);
					mySet.put(mapKey, myTree);
				}
				else{
					myTree = new TreeMap<Integer, String>();
					myTree.put(day, stockPrice);
					mySet.put(mapKey, myTree);
				}
				myTree=null;
				//Now we will have all the values in the hashmap
			}
			myTree=null;
			String temp=null;
			monthVal.clear();
			for(Entry<String, TreeMap> looper : mySet.entrySet()){
				myTree=looper.getValue();
				double maxDay_price=0;
				double minDay_price=0;
				temp=myTree.firstEntry().getValue();
				if(temp!=null){
					minDay_price=Double.parseDouble(temp);
				}

				temp=myTree.lastEntry().getValue();
				if(temp!=null){
					maxDay_price=Double.parseDouble(temp);
				}
				temp=null;
				// Calculate the x(i)=end-begin/begin
				double valuex=(maxDay_price-minDay_price)/minDay_price;
				monthVal.add(valuex);

			}
			mySet.clear();
			int count = monthVal.size();
			double sum=0;
			for(double t:monthVal){
				sum+=t;
			}
			double x_dash=sum/count;
			sum=0;
			for(double t:monthVal){
				sum+=Math.pow((t-x_dash),2);
			}
			if(count>=2){
				sum=sum/(count-1);
			}
			if(sum!=0.0){
			double vol=Math.sqrt(sum);
			DoubleWritable dwValuex=new DoubleWritable(vol);
			context.write(new Text(""+dwValuex), key);
			}
		}
	}
	public static void action(String ... args) {

		try {
			// Create a new Job
			Job job = Job.getInstance();
			job.getConfiguration().set("mapred.compress.map.output", "false");
			job.getConfiguration().setQuietMode(true);
			job.setJarByClass(FirstMapperReducer.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setNumReduceTasks(2);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			/*FileSystem hdfs =FileSystem.get(new Configuration());
			Path workingDir=hdfs.getWorkingDirectory();
			Path newFolderPath= new Path(args[1]);
			newFolderPath=Path.mergePaths(workingDir, newFolderPath);
			if(hdfs.exists(newFolderPath))
			{
			      hdfs.delete(newFolderPath, true); //Delete existing Directory
			}

//			hdfs.mkdirs(newFolderPath);

			//			job.setInputFormatClass(TextInputFormat.class);
			//			job.setOutputFormatClass(TextOutputFormat.class);
			 */
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setJarByClass(FirstMapperReducer.class);
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
