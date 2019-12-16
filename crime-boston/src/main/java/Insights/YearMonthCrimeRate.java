package Insights;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class YearMonthCrimeRate {
    static String path="/home/omnia/Crime_Boston_Big_Data/crime-boston/src/main/java/crimes-in-boston/crime.csv";
    static String out_path ="/home/omnia/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple2<String, String>> csvInput = env.readCsvFile(path)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .includeFields("00000000110000000")
                .types(String.class, String.class);

        GroupReduceOperator<Tuple2<String, String>, Tuple3<String, String, Integer>> crimes_count_rate_per_offence_per_day =
                csvInput.filter(new district_filter_())
                        .groupBy(1,0)

                        .reduceGroup(new month_per_year_count_());

        crimes_count_rate_per_offence_per_day.writeAsCsv(out_path+"crimes_count_rate_month_per_year");
        env.execute("Crimes Count Rate month Per Year");
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        crimes_count_rate_per_offence_per_day.print();
    }
    public static class district_filter_ implements FilterFunction<Tuple2<String ,String>> {

        @Override
        public boolean filter(Tuple2<String, String> record) throws Exception {
            String month = record.f1.toLowerCase();
            if ( month != " " && month != "" && month != "()"){
                return true;


            }
            else
                return false;
        }
    }

    public static class month_per_year_count_ implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String year = null;
            String month = null;
            int cnt = 0;
            // count number of tuples
            for(Tuple2<String, String> r : records) {

                month = r.f1.toLowerCase();
                    year = r.f0.toLowerCase();
                    if (year.length() > 2 && year != " " && year != "" && year != "()") {

                        // increase count
                        cnt++;
                    }

            }

            out.collect(new Tuple3<>(year,month,cnt));
        }
    }
}
