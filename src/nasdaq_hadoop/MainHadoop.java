package nasdaq_hadoop;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MainHadoop {
	public static void main(String ... args) throws Exception {
		String tempFirstOutput = "tempFirst";
//		String tempSecondOutput = "tempSecond";
		
		if(args.length != 2) {
			throw new Exception("Please specify the I/P and O/P Path");
		}
		Configuration conf = new Configuration();
		try{			
			FileSystem.get(conf).delete(new Path(tempFirstOutput),true);
		}catch(Exception e){
			e.printStackTrace();
		}
		long beg=new Date().getTime();
		System.out.println(System.currentTimeMillis());
		FirstMapperReducer.action(args[0], tempFirstOutput);
		SecondMapperReducer.action(tempFirstOutput, args[1]);
		long end=new Date().getTime();
		System.out.println("Time taken is "+(end-beg)/1000);
	}
}