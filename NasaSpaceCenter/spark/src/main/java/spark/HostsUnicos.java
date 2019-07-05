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

public class HostsUnicos {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);

		
		JavaRDD<String> linhas = ctx.textFile("/home/neri/Downloads/logs/access_log_Jul95");
		JavaRDD<String> linhasFiltradas = linhas.filter(s -> s.contains(" 404 "));
		
		

		JavaPairRDD<String, Integer> logNasaAgroupa = linhasFiltradas
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" - - ")[0], 1));
		JavaPairRDD<String, Integer> hosts = logNasaAgroupa.reduceByKey((x, y) -> x + y);
		List<Tuple2<String, Integer>> lista = hosts.collect();
		List<String> hostsUnicos = new ArrayList<>();
		List<String> hostsrep = new ArrayList<>();
		for (Tuple2<String, Integer> registro : lista) {
			if (!hostsUnicos.contains(registro._1())) {
				hostsUnicos.add(registro._1());
			}
		}
		System.out.println("Total de hosts unicos : " + hostsUnicos.size());
	}
}
