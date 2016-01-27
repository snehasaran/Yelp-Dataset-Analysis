import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.opencsv.CSVParser;

// Find top businesses in Nightlife, Active Life, Bars, Home Services,
// Health & Medical Categories in Vegas based on user reviews or review count/stars and business attributes. 


public class findTopBusiness {

	public static class findBusinessMapper extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String city = "Las Vegas";
			String businessId;
			String businessName;
			String star;
			String review_count;
			String category;
			String Mapperkey = null,Mappervalue=null;

			CSVParser csv = new CSVParser(',');
			String[] nextLine = csv.parseLine(value.toString());

			/*key = business type
					value = business id + review_count + stars

					hm.put(businessid,(review_count;stars))
					then retrieve the value based on business id and split the value on ;*/

			try{
				businessId = nextLine[1];
				businessName = nextLine[2];
				star = nextLine[4];
				review_count=nextLine[5];
				category = nextLine[6];
				System.out.println(category);
				if(category.contains("Nightlife")){
					Mapperkey = "Nightlife";

					Mappervalue = businessName +";"+ star + ";"+ review_count + ";" + "Nightlife";
					context.write(new Text(Mapperkey), new Text(Mappervalue));
				}

				if(category.contains("Active Life")){
					Mapperkey = "Active Life";

					Mappervalue = businessName +";"+ star + ";"+ review_count +";Active Life";
					context.write(new Text(Mapperkey), new Text(Mappervalue));
				}

				if(category.contains("Home Services")){
					Mapperkey = "Home Services";

					Mappervalue = businessName +";"+ star + ";"+ review_count+";Home Services";
					context.write(new Text(Mapperkey), new Text(Mappervalue));
				}

				if(category.contains("Health & Medical")){
					Mapperkey = "Health & Medical";

					Mappervalue = businessName +";"+ star + ";"+ review_count+";Health & Medical";
					context.write(new Text(Mapperkey), new Text(Mappervalue));
				}
				
				if(category.contains("Bars")){
					Mapperkey = "Bars";

					Mappervalue = businessName +";"+ star + ";"+ review_count+";Bars";
					context.write(new Text(Mapperkey), new Text(Mappervalue));
				}
				
				


			}
			catch(Exception e){
				e.printStackTrace();
				System.exit(0);
			}
		}

	}



	public static class findBusinessReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/*ArrayList<String> businessIdAL = new ArrayList<>();
			ArrayList<String> businessNameAL = new ArrayList<>();
			ArrayList<String> starAL = new ArrayList<>();
			ArrayList<String> reviewCountAL = new ArrayList<>();*/
			int reviewCount = 1130;
			float starRating = 3.5f;


			ArrayList<String> quad1= new ArrayList<>();
			ArrayList<String> quad2= new ArrayList<>();
			ArrayList<String> quad3= new ArrayList<>();
			ArrayList<String> quad4= new ArrayList<>();

			String category = null;
			for(Text v : values){
				String valString = v.toString();
				String[] valArray = valString.split(";");
				category = valArray[3];
				//Quadrant1
				if(Integer.parseInt(valArray[2]) > reviewCount && (Float.parseFloat(valArray[1]) > starRating)){
					quad1.add(valArray[0]);
				}

				//Quad4
				if(Integer.parseInt(valArray[2]) > reviewCount && (Float.parseFloat(valArray[1]) < starRating)){
					quad4.add(valArray[0]);
				}

				//Quad3
				if(Integer.parseInt(valArray[2]) < reviewCount && (Float.parseFloat(valArray[1]) < starRating)){
					quad3.add(valArray[0]);
				}

				//Quad2
				if(Integer.parseInt(valArray[2]) < reviewCount && (Float.parseFloat(valArray[1]) > starRating)){
					quad2.add(valArray[0]);
				}


			}


			String keyQuad1 = 1 + "";
			context.write(new Text("Category"), new Text(category));
			for(String s : quad1){
				context.write(new Text(keyQuad1), new Text(s));
			}

			String keyQuad2 = 2 + "";
			for(String s : quad2){
				context.write(new Text(keyQuad2), new Text(s));
			}

			String keyQuad3 = 3 + "";
			for(String s : quad3){
				context.write(new Text(keyQuad3), new Text(s));
			}

			String keyQuad4 = 4 + "";
			for(String s : quad4){
				context.write(new Text(keyQuad4), new Text(s));
			}


		}//end of reduce function
	
	}//end of reduce class
	

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"findTopBusiness");
		job.setJarByClass(findTopBusiness.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(5);

		job.setMapperClass(findBusinessMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(findBusinessReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}//end of main

}//end of class


