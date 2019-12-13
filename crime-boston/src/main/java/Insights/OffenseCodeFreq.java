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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class OffenseCodeFreq {

	static String out_path ="/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple1<String>> csvInput = env.readCsvFile("/home/rana/Documents/BigData/Project/Crime_Boston_Big_Data/crime-boston/src/main/java/crimes-in-boston/crime.csv")
				.ignoreFirstLine()
				.includeFields("010000000000000")
				.types(String.class);

		DataSet<Tuple2<String, Integer>> counts =
				csvInput.flatMap(new Tokenizer())
						.groupBy(0)
						.sum(1);

		counts.writeAsCsv(out_path+"offense_code_frequency.csv");

		env.execute("Offense Code Frequency");
	}

	public static final class Tokenizer implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {

			String[] tokens = value.toString().toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
