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
import org.graphframes.GraphFrame;

import java.util.List;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		JavaRDD<String> vertex = ctx.textFile("src/main/resources/wiki-vertices.txt");
		JavaRDD<String> edge = ctx.textFile("src/main/resources/wiki-edges.txt");

		List<StructField> ListVertex = Lists.newArrayList();
		ListVertex.add(DataTypes.createStructField("id",DataTypes.LongType,false));
		ListVertex.add(DataTypes.createStructField("name",DataTypes.StringType,false));
		StructType forVertex = DataTypes.createStructType(ListVertex);

		List<StructField> ListEdge = Lists.newArrayList();
		ListEdge.add(DataTypes.createStructField("src",DataTypes.LongType,false));
		ListEdge.add(DataTypes.createStructField("dst",DataTypes.LongType,false));
		StructType forEdge = DataTypes.createStructType(ListEdge);

		Dataset<Row> V = sqlCtx.createDataFrame(vertex.map(v ->
		RowFactory.create(Long.parseLong(v.split("\t")[0]),v.split("\t")[1])),forVertex);
        Dataset<Row> E = sqlCtx.createDataFrame(edge.map(e ->
		RowFactory.create(Long.parseLong(e.split("\t")[0]),Long.parseLong(e.split("\t")[1]))),forEdge);
        GraphFrame graph = GraphFrame.apply(V,E);

        org.graphframes.lib.PageRank pgRank = graph.pageRank().resetProbability(0.15).maxIter(10);
        GraphFrame pgRankGraph = pgRank.run();
        for (Row rname : pgRankGraph.vertices().sort(org.apache.spark.sql.functions.desc("Pagerank for the given dataset is:")).toJavaRDD().take(10)) 
		{
	    System.out.println(rname.getString(1));
        }	
}
	
}
