package spark;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TotalErro404 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);

		
		JavaRDD<String> linhas = ctx.textFile("/home/neri/Downloads/logs/access_log_Jul95");
		JavaRDD<String> linhasFiltradas = linhas.filter(s -> s.contains(" 404 "));
		
		// O total de erros 404
		List<String> resultados = linhasFiltradas.collect();
		System.out.println("O total de erros 404. Access_log_Jul9 : "+resultados.size());
		

		JavaRDD<String> linhas1 = ctx.textFile("/home/neri/Branchs/spark/spark/logs/access_log_Aug95");
		JavaRDD<String> linhasFiltradas1 = linhas1.filter(s -> s.contains(" 404 "));
		
		// O total de erros 404
		List<String> resultados1 = linhasFiltradas1.collect();
		System.out.println("O total de erros 404. access_log_Aug95 : "+resultados1.size());

	}

}
