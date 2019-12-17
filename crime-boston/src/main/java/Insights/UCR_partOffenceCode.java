package Insights;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class UCR_partOffenceCode {


        static String path="/home/omnia/Crime_Boston_Big_Data/crime-boston/src/main/java/crimes-in-boston/crime.csv";
        static String out_path ="/home/omnia/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";

        public static void main(String[] args) throws Exception {
            // set up the batch execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSource<Tuple2<String, String>> csvInput = env.readCsvFile(path)
                    .ignoreFirstLine()
                    .parseQuotedStrings('"')
                    .includeFields("01000000000010000")
                    .types(String.class, String.class);

            GroupReduceOperator<Tuple2<String, String>, Tuple2<String, Integer>> crimes_count_rate_per_offence_per_day =
                    csvInput.filter(new district_filter_())
                            .groupBy(1)
                            .reduceGroup(new offence_code_per_day_count_());
//            DataSet<Tuple2<String, Integer>> crimes_per_year =
//                    // split up the lines in pairs (2-tuples) containing: (word,1)
//                    csvInput.flatMap(new Tokenizer())
//                            // group by the tuple field "0" and sum up tuple field "1"
//                            .groupBy(1)
//                            .sum(1);

            crimes_count_rate_per_offence_per_day.writeAsCsv(out_path+"ucr_part_count_different_offence_code.csv");
            env.execute("Crimes Count Rate Per Offence Per District");
        }

        public static class district_filter_ implements FilterFunction<Tuple2<String ,String>> {

            @Override
            public boolean filter(Tuple2<String, String> record) throws Exception {
                return (record.f1.length()>0&&record.f1!=""&&record.f1!=" ");
            }
        }

    public static final class Tokenizer implements FlatMapFunction<Tuple2<String,String>, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Tuple2<String,String> value, Collector<Tuple2<String, Integer>> out) throws Exception {

            String tokens = value.f1.toString().toLowerCase();
            String token2 = value.f0.toString().toLowerCase();
            if ((tokens.length() > 2  &&  tokens.length() > 0)&&  tokens !=" " &&
                    tokens !="" && tokens !="()"&&token2.length()>0&&token2!=" ")
                out.collect(new Tuple2<>(tokens, 1));


        }
    }
        public static class offence_code_per_day_count_ implements
                GroupReduceFunction<Tuple2<String ,String>, Tuple2<String, Integer>> {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
                Set<String> hash_Set = new HashSet<String>();
                String offenceCode = null;
                String district = null;
                int cnt = 0;
                // count number of tuples
                for(Tuple2<String, String> r : records) {

                    district = r.f1.toLowerCase();
                    offenceCode = r.f0;
                    hash_Set.add(offenceCode);
                    cnt++;
                }
                out.collect(new Tuple2<>(district,hash_Set.size()));
                
            }
        }


}
