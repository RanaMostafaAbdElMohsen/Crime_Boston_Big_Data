package Insights;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CountRatePerOffencePerDay {
	static String path="/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/crimes-in-boston/crime.csv";
	static String out_path ="/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";


	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<String, String>> csvInput = env.readCsvFile(path)
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.includeFields("01000000001000000")
				.types(String.class, String.class);

		GroupReduceOperator<Tuple2<String, String>, Tuple3<String, String, Integer>> crimes_count_rate_per_offence_per_day =
		csvInput.groupBy(1,0)
		.reduceGroup(new offence_code_per_day_count_());

		crimes_count_rate_per_offence_per_day.writeAsCsv(out_path+"crimes_count_rate_per_offence_per_day_1");
		env.execute("Crimes Count Rate Per Offence Per Day");
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		crimes_count_rate_per_offence_per_day.print();
	}

	public static class offence_code_per_day_count_ implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String offenceCode = null;
			String day = null;
			int cnt = 0;
			// count number of tuples
			for(Tuple2<String, String> r : records) {

				day = r.f1.toLowerCase();
				if (day.length() > 2 && day != " " && day != "" && day != "()")
				{
					offenceCode = r.f0;
					if(offenceCode.length() == 5)
						offenceCode = offenceCode.substring(1);
					// increase count
					cnt++;
				}
			}
			out.collect(new Tuple3<>(offenceCode,day,cnt));
		}
	}

}