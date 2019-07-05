package spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CincoUrlsQueMaisCausaram404 {

	public static void main(String[] args) {
		CincoUrlsQueMaisCausaram404 cincoUrlsJul95 = new CincoUrlsQueMaisCausaram404();
		SparkConf confJul95 = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctxJul95= new JavaSparkContext(confJul95);
		System.out.println("Os cinco hosts que mais causaram erro 404 no log access_log_Jul95 são :");
		List<String> valueList = new ArrayList<>();
		valueList = cincoUrlsJul95.getCincoUrlsQueMaisCausaram404("/home/neri/Branchs/spark/spark/logs/access_log_Jul95",confJul95,ctxJul95);
		for (String string : valueList) {
			System.out.println(string);
		}
		
		CincoUrlsQueMaisCausaram404 cincoUrlsAug95 = new CincoUrlsQueMaisCausaram404();
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NasaLog");
		JavaSparkContext ctx= new JavaSparkContext(conf);
		List<String> valueListAug95 = new ArrayList<>();
		System.out.println("Os cinco hosts que mais causaram erro 404 no log aaccess_log_Aug95 são :");
		valueListAug95 = cincoUrlsAug95.getCincoUrlsQueMaisCausaram404("/home/neri/Branchs/spark/spark/logs/access_log_Aug95",conf,ctx);
		for (String string : valueListAug95) {
			System.out.println(string);
		}
		
	}
	
	/**
	 * By Thiago Neri
	 * @param output
	 * @param conf
	 * @param ctx
	 * @return List<String>
	 */
	public  List<String> getCincoUrlsQueMaisCausaram404(String output,SparkConf conf,JavaSparkContext ctx) {
    	

		
		JavaRDD<String> linhasJul95 = ctx.textFile(output);
		JavaRDD<String> linhasFiltradasJul95 = linhasJul95.filter(s -> s.contains(" 404 "));
		
		JavaPairRDD<String, Integer> logNasaAgroupaJul95 = linhasFiltradasJul95
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" - - ")[0], 1));
		
		List<Tuple2<String, Integer>> listaJul95 = logNasaAgroupaJul95.collect();
		List<String> hostsUnicosJul95 = new ArrayList<>();
		List<String> listDiaJul95 = new ArrayList<>();
		for (Tuple2<String, Integer> registro : listaJul95) {
			listDiaJul95.add(registro._1());
	    }
		Set<String> listSetJul95 = new HashSet<String>();
		List<String> listSort = new ArrayList<String>();
		Map<String,Integer> mapListJul95 = new HashMap<String,Integer>();
		//System.out.println("Log "+output.split("access")[0]);
 		for (String string : listDiaJul95) {
 			mapListJul95.put(string, Collections.frequency(listDiaJul95, string));
		}
 		
 		
 		Map sortedMapul95 = new TreeMap(new ValueComparator(mapListJul95));
 		for (Iterator iter = mapListJul95.keySet().iterator(); iter.hasNext();) {
 			String key = (String) iter.next();
 			sortedMapul95.put(key, mapListJul95.get(key));
 		}
 		
	 	Set<String> chaves =  sortedMapul95.keySet();
	 	List<String> valueList = new ArrayList<>();
	 	int count = 0;
	 	for (String chave : chaves) {
	 		count++;
			if(count<=5) {
				valueList.add(sortedMapul95.get(chave)+" "+chave);
			}
			
		}
	 	ctx.close();
	 	return valueList;
	 	 
	}

}
