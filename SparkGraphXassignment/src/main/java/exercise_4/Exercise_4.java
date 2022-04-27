package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// import org.apache.spark.sql.functions.*;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.*;

import scala.collection.immutable.Map;
import java.lang.Math;
import java.util.List;

public class Exercise_4 {
	
	public static Double percent_change(Double current_value, Double old_value) {
		return 100 * Math.abs(current_value - old_value) / old_value;
	}	 

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) { //JavaSparkContext:Java-friendly version of SparkContext that returns JavaRDDs and works with Java collections instead of Scala ones
		//SQLContext:entry point for working with structured data(rows&columns)in Spark
		JavaRDD<String> vertex = ctx.textFile("SparkGraphXassignment/src/main/resources/wiki-vertices.txt"); //getting the data for vertices from this file
		JavaRDD<String> edge = ctx.textFile("SparkGraphXassignment/src/main/resources/wiki-edges.txt"); //getting the data for edges from this file

		List<StructField> ListVertex = Lists.newArrayList(); //StructField class is used to programmatically specify the schema to the DataFrame
		ListVertex.add(DataTypes.createStructField("id",DataTypes.LongType,false));
		ListVertex.add(DataTypes.createStructField("title",DataTypes.StringType,false));
		StructType forVertex = DataTypes.createStructType(ListVertex); //StructType is a collection of StructField

		List<StructField> ListEdge = Lists.newArrayList();
		ListEdge.add(DataTypes.createStructField("src",DataTypes.LongType,false));
		ListEdge.add(DataTypes.createStructField("dst",DataTypes.LongType,false));
		StructType forEdge = DataTypes.createStructType(ListEdge);

		Dataset<Row> v1 = sqlCtx.createDataFrame(vertex.map(v ->  //Dataset is a distributed collection of data
		RowFactory.create(Long.parseLong(v.split("\t")[0]),v.split("\t")[1])),forVertex); //A factory class used to construct Row objects
        Dataset<Row> e1 = sqlCtx.createDataFrame(edge.map(e ->
		RowFactory.create(Long.parseLong(e.split("\t")[0]),Long.parseLong(e.split("\t")[1]))),forEdge);
        GraphFrame graph = GraphFrame.apply(v1,e1);

		// Damping_factor = 1 - reset_probability
		Double resetProbability = 0.85;

		Integer maxIter = 10;

        // org.graphframes.lib.PageRank pgRank = graph.pageRank().resetProbability(resetProbability).maxIter(10);
		// // Run PageRank for a fixed number of iterations returning a graph with vertex attributes containing the PageRank and edge 
		// // attributes the normalized edge weight.
        // GraphFrame pgRankGraph = pgRank.run(); 
        // for (Row rname : pgRankGraph.vertices().sort(org.apache.spark.sql.functions.desc("Pagerank")).toJavaRDD().take(10)) 
		// {
	    // System.out.println(rname.getString(1));
        // }
		
		Double damp_fact_list[] = {0.70, 0.75, 0.85, 0.90};
		Double resetProb;
		Double pg_val_previos = 1.0, pg_val_current = 0.0;
		long time_taken; // System.currentTimeMillis();
		int i = 0, j = 0;

		// Run pregal for these damping factor values
		for (i = 0; i < damp_fact_list.length; i++) {

			resetProb = 1 - damp_fact_list[i];
			// Run pagerank for these iterations
			for (j = 5; j < 20; j++) {

				org.graphframes.lib.PageRank pgRank2 = graph.pageRank().resetProbability(resetProb).maxIter(j);
				// Run PageRank for a fixed number of iterations returning a graph with vertex attributes containing the PageRank and edge 
				// attributes the normalized edge weight.
				long start_time = System.currentTimeMillis();

				GraphFrame pgRankGraph2 = pgRank2.run(); 
				pg_val_current = pgRankGraph2.vertices().orderBy(org.apache.spark.sql.functions.desc("Pagerank")).limit(10).agg(sum("pagerank")).first().getDouble(0);
				
				long end_time = System.currentTimeMillis() - start_time;
				
				// Detects if difference in current pregal value and previous pregal value is less than 5% 
				// System.out.println( "percent_val: " + percent_change(pg_val_current, pg_val_previos) );
				if ( percent_change(pg_val_current, pg_val_previos) < 0.5) {
					System.out.println("----- !!! The PageRank has converged !!! -----");
					System.out.println("----- Time taken for this run: " + end_time + " MiliSeconds");
					break;
				} 
				pg_val_previos = pg_val_current;
				System.out.println("damping_factor: " + damp_fact_list[i]);
				System.out.println("For i: " + j + " Pregal_value: " + pg_val_current);
			}
		}

		// if (percent_change(5555.0, 5555.0) < 5.0) {
		// 	System.out.println("If works");
		// }

		// System.out.println( percent_change(5555.0, 1.0) );

	}

}
