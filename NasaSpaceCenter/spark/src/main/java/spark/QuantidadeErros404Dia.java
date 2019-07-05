package spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class QuantidadeErros404Dia {

	public static void main(String[] args) {

		QuantidadeErros404Dia quantidadeErrossul95 = new QuantidadeErros404Dia();
		SparkConf confJul95 = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctxJul95= new JavaSparkContext(confJul95);
		Set<String> valueList =  new HashSet<String>();
		valueList = quantidadeErrossul95.getQuantidadeErros404Dia("/home/neri/Branchs/spark/spark/logs/access_log_Jul95",confJul95,ctxJul95);
		for (String string : valueList) {
			System.out.println(string);
		}
		
		QuantidadeErros404Dia quantidadeErrossAug95 = new QuantidadeErros404Dia();
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctx= new JavaSparkContext(conf);
		Set<String> valueListAug95 =  new HashSet<String>();
		
		valueListAug95 = quantidadeErrossAug95.getQuantidadeErros404Dia("/home/neri/Branchs/spark/spark/logs/access_log_Aug95",conf,ctx);
		for (String string : valueListAug95) {
			System.out.println(string);
		}
	}
	
	
	
	/**
	 * By Thiago Neri
	 * @param output
	 * @param conf
	 * @param ctx
	 * @returnSet<String>
	 */
	public  Set<String> getQuantidadeErros404Dia(String output,SparkConf conf,JavaSparkContext ctx) {
		
 		JavaRDD<String> linhasAug95 = ctx.textFile(output);
		JavaRDD<String> linhasFiltradasAug95 = linhasAug95.filter(s -> s.contains(" 404 "));
		
		JavaPairRDD<String, Integer> logNasaAgroupaAug95 = linhasFiltradasAug95
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" - - ")[1].split(":",7)[0].replace("[",""), 1));
		
		List<Tuple2<String, Integer>> listaAug95 = logNasaAgroupaAug95.collect();
	    List<String> listDiaAug95 = new ArrayList<>();
		for (Tuple2<String, Integer> registro : listaAug95) {
			listDiaAug95.add(registro._1().split("/")[0]);
	    }
		Set<String> listSetAug95 = new HashSet<String>();
		System.out.println(output);
 		for (String string : listDiaAug95) {
 			listSetAug95.add("Dia :"+ string + " Quantidade de Erro 404 :"+Collections.frequency(listDiaAug95, string));
		}
 	
 		ctx.close();
	 	return listSetAug95;
	 	 
	}
}
