
public static class MinMaxCountMapper extends
	Mapper<Object, Text, Text, MinMaxCountTuple> {

	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();

	private final static SimpleDateFormat frmt = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH::mm:ss.SSS");

	public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {

		Map<String, String> parsed = transformXmlToMap(value.toString());

		String strDate = parsed.get("CreationDate");
		String userId = parsed.get("UserId");
		Date creationDate = frmt.parse(strDate);

		outTuple.setMin(creationDate);
		outTuple.setMax(creationDate);
		outTuple.setCount(1);

		outUserId.set(userId);

		context.write(outUserId, outTuple);
	}
}

public static class MinMaxCountReducer extends
	Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

	private MinMaxCountTuple result = new MinMaxCountTuple();

	public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context)
		throws IOException, InterruptedException {

		result.setMin(null);
		result.setMax(null);
		result.setCount(0);
		int sum = 0;

		for (MinMaxCountTuple val : values) {
			if (result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {
				result.setMin(val.getMin());
			}

			if (result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
				result.setMax(val.getMax());
			}
			sum += val.getCount();
		}
		result.setCount(sum);
		context.write(key, result);
	}
}