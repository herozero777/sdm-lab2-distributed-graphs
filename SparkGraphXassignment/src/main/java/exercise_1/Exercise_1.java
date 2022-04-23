package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {

    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable { //implementing serializable interface
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            if (message == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                return Math.max(vertexValue,message);
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // then do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            } else {
                // if source vertex value is greater than destination vertex value then, propagate source vertex value
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.max(o,o2); //this method returns the greater of two integer values
        }
    }

    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(     //List in Java provides the facility to maintain the ordered collection. It contains the index-based methods to insert, update, delete and search the elements. It can have the duplicate elements also. We can also store the null elements in the list.
            new Tuple2<Object,Integer>(1l,9), // (vertexID,value)
            new Tuple2<Object,Integer>(2l,1), 
            new Tuple2<Object,Integer>(3l,6),
            new Tuple2<Object,Integer>(4l,8)
        );
        for(Tuple2<Object, Integer> vertex:vertices)
        System.out.println(vertex);

        List<Edge<Integer>> edges = Lists.newArrayList(
            new Edge<Integer>(1l,2l, 1), //indicate the edge from vertex1 to vertex2 with weight 1
            new Edge<Integer>(2l,3l, 1), //indicate the edge from vertex2 to vertex3 with weight 1
            new Edge<Integer>(2l,4l, 1), //indicate the edge from vertex2 to vertex4 with weight 1
            new Edge<Integer>(3l,4l, 1), //indicate the edge from vertex3 to vertex4 with weight 1
            new Edge<Integer>(3l,1l, 1)  //indicate the edge from vertex3 to vertex1 with weight 1
        );
        for(Edge<Integer> edge:edges)
        System.out.println(edge);

        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
    
        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        Tuple2<Long,Integer> max = (Tuple2<Long,Integer>)ops.pregel(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,      // Run until convergence
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
        .vertices().toJavaRDD().first();

        System.out.println(max._2 + " is the maximum value in the graph");
	}
	
}
