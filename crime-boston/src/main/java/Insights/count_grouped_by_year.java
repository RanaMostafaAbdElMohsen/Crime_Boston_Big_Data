package Insights;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.GroupReduceFunction;

public class count_grouped_by_year {
    static String path="/home/rematchka/Documents/crimes-in-boston/crime.csv";
    static String out_path ="/home/rematchka/Documents/Crime_Boston_Big_Data/crime-boston/src/main/java/Insights/";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * http://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */


        // Number of crimes in each street

        DataSource<Tuple2<String,String>> csvInput = env.readCsvFile(path)
                .ignoreFirstLine()
                .includeFields("10000000100000000")
                .types(String.class,String.class);

        GroupReduceOperator<Tuple2<String, String>, Tuple3<String, String, Integer>> group_incident_by_year =
                csvInput.groupBy(0,1)
                        .reduceGroup(new CrimeCounter());







        final ParameterTool params = ParameterTool.fromArgs(args);
        group_incident_by_year.writeAsCsv(out_path+"group_by_year_on_incident_number_and_count");
        env.execute("Crimes Files Frequency");
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        group_incident_by_year.print();




    }

    public static class CrimeCounter implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String crimerecord = null;
            String year = null;
            int cnt = 0;
            // count number of tuples
            for(Tuple2<String, String> m : records) {
                crimerecord = m.f0;
                year = m.f1;
                // increase count
                cnt++;
            }
            // emit crimerecord, ucr_code, and count
            out.collect(new Tuple3<>(crimerecord, year, cnt));
        }
    }

//    public static final class Tokenizer implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {
//
//        @Override
//        public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//            String[] tokens = value.toString().toLowerCase().split("\n");
//
//            // emit the pairs
//            for (String token : tokens) {
//                if (token.length() > 0) {
//
//                    out.collect(new Tuple2<>(token, 1));
//                }
//            }
//        }
//    }

}
