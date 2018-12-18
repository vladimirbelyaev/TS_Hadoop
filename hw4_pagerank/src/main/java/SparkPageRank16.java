import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;


public class SparkPageRank16 {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Invalid number of parameters");
            System.exit(1);
        }
        Double N = 4847571.0;
        double alpha = 0.01;
        final SparkConf conf = new SparkConf()
                //.setMaster("local[*]")
                .setAppName("SparkPR");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        Accumulator<Double> lostPR = sc.accumulator(0.0, new OwnDoubleAccumulator());
        JavaPairRDD<Long, Iterable<Long>> graph = lines.filter(s -> !s.substring(0,1).equals("#"))
                .flatMapToPair(new PairFlatMapFunction<String, Long, Iterable<Long>>() {
                    @Override
                    public Iterable<Tuple2<Long, Iterable<Long>>> call(String s) throws Exception {
                        String[] tmp = s.split("\t");
                        ArrayList<Long> realVal = new ArrayList<>();
                        ArrayList<Long> fakeVal = new ArrayList<>();
                        realVal.add(Long.parseLong(tmp[1]));
                        realVal.add((long)-1);
                        fakeVal.add(((long)-1));
                        ArrayList<Tuple2<Long, Iterable<Long>>> res = new ArrayList<>();
                        res.add(new Tuple2<>(Long.parseLong(tmp[0]), realVal));
                        res.add(new Tuple2<>(Long.parseLong(tmp[1]), fakeVal));
                        return res;
                    }
                }).reduceByKey(new Function2<Iterable<Long>, Iterable<Long>, Iterable<Long>>() {
                    @Override
                    public Iterable<Long> call(Iterable<Long> v1, Iterable<Long> v2) throws Exception {
                        HashSet<Long> outSet = new HashSet<>();
                        for (Long val: v1){
                            outSet.add(val);
                        }
                        for (Long val: v2) {
                            outSet.add(val);
                        }
                        return new ArrayList<>(outSet);
                    }
                }).cache();
        JavaPairRDD<Long, Double> PR = graph.mapValues(a -> 1.0/N);

        for (int i = 0; i < 7; i++) {
            Double lostPRd = lostPR.value();
            lostPR.zero();
            JavaPairRDD<Long, Tuple2<Iterable<Iterable<Long>>, Iterable<Double>>> grp = graph.cogroup(PR);
            JavaPairRDD<Long, Double> mapped = grp.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<Iterable<Long>>, Iterable<Double>>>, Long, Double>() {
                @Override
                public Iterable<Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Iterable<Iterable<Long>>, Iterable<Double>>> longTuple2Tuple2) throws Exception {
                    Long counter = 0L;
                    for (Long uid : longTuple2Tuple2._2()._1().iterator().next()) {
                        if (uid != -1) {
                            counter += 1;
                        }
                    }
                    Double val;
                    if (longTuple2Tuple2._2()._2().iterator().hasNext()) {
                        val = longTuple2Tuple2._2()._2().iterator().next() + lostPRd / N;
                    } else{
                        val = lostPRd/N;
                    }
                    ArrayList<Tuple2<Long, Double>> res = new ArrayList<>();
                    if (counter == 0) {
                        lostPR.add(val);
                        return res;
                    }
                    val /= counter;
                    for (Long uid : longTuple2Tuple2._2()._1().iterator().next()) {
                        if (uid != -1) {
                            res.add(new Tuple2<>(uid, val));
                        }
                    }
                    return res;
                }
            });
            PR = mapped.reduceByKey((a, b) -> (a + b)).mapValues(w -> (1 - alpha) * w + alpha / N);
        }
        JavaPairRDD<Double, Long> PRtoSort = PR.mapToPair(a->a.swap());
        PRtoSort.sortByKey(false).saveAsTextFile(args[1] + "spark/sortedPR");
        sc.stop();
    }
}