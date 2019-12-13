/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Insights;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class OffenceCodeFreqPerDistrict {

	static String path="/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/crimes-in-boston/crime.csv";
	static String out_path ="/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";


	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<String, String>> csvInput = env.readCsvFile(path)
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.includeFields("01001000000000000")
				.types(String.class, String.class);

		GroupReduceOperator<Tuple2<String, String>, Tuple3<String, String, Integer>> crimes_count_rate_per_offence_per_day =
				csvInput.filter(new district_filter_())
						.groupBy(1,0)
						.reduceGroup(new offence_code_per_day_count_());

		crimes_count_rate_per_offence_per_day.writeAsCsv(out_path+"crimes_count_rate_per_offence_per_district.csv");
		env.execute("Crimes Count Rate Per Offence Per District");
	}

	public static class district_filter_ implements FilterFunction<Tuple2<String ,String>> {

		@Override
		public boolean filter(Tuple2<String, String> record) throws Exception {
			return record.f1.length()>0;
		}
	}

	public static class offence_code_per_day_count_ implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String offenceCode = null;
			String district = null;
			int cnt = 0;
			// count number of tuples
			for(Tuple2<String, String> r : records) {

				district = r.f1.toLowerCase();
				offenceCode = r.f0;
				cnt++;

			}
			out.collect(new Tuple3<>(offenceCode,district,cnt));
		}
	}
}
