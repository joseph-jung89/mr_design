import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

// this is fucking comical as well.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringEscapeUtils;

public class CommentWordCount {

	public static class WordCountMapper 
		extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		// why do you have to set this here?
		private Text word = new Text();

		// data is already an iterable object, plz tell me how. goddamn it
		// useless transforms and cleaning up and stuff, not very useful
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

				// and this is map. why? 
				Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

				String txt = parsed.get("Text");
				if (txt != null) {
					txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
					txt = txt.replaceAll("'", "").replaceAll("[^a-zA-Z]", " ");
					
					StringTokenizer itr = new StringTokenizer(txt);
					while (itr.hasMoreTokens()) {
						word.set(itr.nextToken());
						context.write(word, one);
					}
				}
			}
	}

	public static class IntSumRecuder
		extends Reducer<Text, IntWritable, Text, IntWritable> {
			
			private IntWritable result = new IntWritable();

			// why iterable of intwritables? and is java so thick that..
			// I bet ya this reduce function is overloaded beyond repair.
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				// one important detail though, combiner is expected to funnel all same word counts
				// to this function. so that in reducer job has to be done only once.
				result.set(sum);
				context.write(key, result);
			}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			throw Exception("wrong invocation of the job");
		}

		// this shit is fucking awful, but it's what they do in java
		// main driver settings, input, output, their types, mapper, combiner, reducer
		Job job = new Job(conf, "SO comment word count");
		job.setJarByClass(CommentWordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}