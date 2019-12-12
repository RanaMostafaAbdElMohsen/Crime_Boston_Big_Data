package Insights;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;


public class not_safest_street {


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

        DataSource<Tuple1<String>> csvInput = env.readCsvFile(path)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .includeFields("00000000000001000")
                .types(String.class);

        DataSet<Tuple2<String, Integer>> counts_Street_crimes =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                csvInput.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1)
                        .maxBy(1);






//        final ParameterTool params = ParameterTool.fromArgs(args);
        counts_Street_crimes.writeAsCsv(out_path+"not_safest_street");
        env.execute("Not Safest Street");
//        System.out.println("Printing result to stdout. Use --output to specify output path.");
//        counts_Street_crimes.print();




    }

    public static final class Tokenizer implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {

            String tokens = value.toString().toLowerCase();
            if ((tokens.length() > 2  &&  tokens.length() > 0)&&  tokens !=" " &&   tokens !="" && tokens !="()")
                out.collect(new Tuple2<>(tokens, 1));


        }
    }
}
