package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TotalBytesRetornados {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		JavaRDD<String> linhas = ctx.textFile("/home/neri/Downloads/logs/access_log_Jul95");
		JavaRDD<String> linhasFiltradasJul95 = linhas.filter(s -> s.contains(" 200 "));
		JavaPairRDD<String, Integer> logNasaAgroupa = linhasFiltradasJul95
				.mapToPair(s -> new Tuple2<String, Integer>(s.split("")[3], 1));
		
		List<Tuple2<String, Integer>> lista = logNasaAgroupa.collect();
		List<String> hostsUnicos = new ArrayList<>();
		List<String> hostsrep = new ArrayList<>();
		for (Tuple2<String, Integer> registro : lista) {
			System.out.println(registro._1());
		}
		//System.out.println("Total de hosts unicos : " + hostsUnicos.size());
	}
}
