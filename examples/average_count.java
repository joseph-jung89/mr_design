
public static class AverageMapper extends
	Mapper<Object, Text, IntWritable, CountAverageTuple> {

	private IntWritable outHour = new IntWritable();
	private CountAverageTuple outCountAverage = new CountAverageTuple();
	private final static SimpleDateFormat frmt = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH::mm:ss.SSS");

	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {

		Map<String, String> parsed = transformXmlToMap(value.toString());

		String strDate = parsed.get("CreationDate");
		String text = parsed.get("Text");
		Date creationDate = frmt.parse(strDate);
		outHour.set(creationDate);

		outCountAverage.setAverage(text.length);
		outCountAverage.setCount(1);

		context.write(outHour, outCountAverage);
	}
}

public static class AverageReducer extends
	Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

	private CountAverageTuple result = new CountAverageTuple();

	public void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context)
		throws IOException, InterruptedException {

		float sum = 0;
		float count = 0;

		for (CountAverageTuple val : values) {
			sum += val.getCount() * val.getAverage();
			count += val.getCount();
		}
		result.setCount(count);
		result.setAverage(sum / count);

		context.write(key, result);
	}
}