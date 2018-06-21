---
title: "Big Data Graph Analytics in Python Using GraphFrames for Apache Spark"
date: 2018-06-03 00:00:00
categories: networks, graphs, big data
excerpt: "In this post, you will learn how to read a graph from a file and how to implement different graph algorithms using GraphFrames in PySpark."
header:
  overlay_image: /assets/images/posts/graphframes/header.jpg
  overlay_filter: rgba(0, 0, 0, 0.65)
---

[GraphFrames](http://graphframes.github.io/) is a graph-parallel processing library for Apache Spark. In contrast to [GraphX](https://spark.apache.org/graphx/), which is built on top of the resilient distributed dataset (RDD), GraphFrames is based on DataFrames. The DataFrame is a data structure providing a higher level of abstraction and better performance compared to the RDD as result of defining a schema for the data in advance. Another advantage of GraphFrames is that it is currently available for Python, while GraphX is not, [and probably won't be ever](https://issues.apache.org/jira/browse/SPARK-3789). 

## Reading a Graph From a File
To create a graph using GraphFrames, we need to build two DataFrames: one for vertices and one for edges. There are many different file formats for storing graphs. In this section, we consider the format used by the [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/), a text file with a line for each edge indicating the source and the target vertices separated by a whitespace character. Optionally, the file can contain comment lines starting with the hash symbol.

We start by reading the edges of the graph. Since the file format has a simple tabular structure, we can use the **read.csv** method from the **SparkSession** object. Specifying the file path, the column separator character, and the comment character is straightforward. The **read.csv** method is smart enough to infer the structure of the data, but explicitly specifying the schema avoids an additional pass over the data. In addition, it allows naming the columns and declaring their type. GraphFrames requires a DataFrame with a row for each edge with a column **src** indicating the source vertex id and a column **dst** indicating the destination vertex id of the edge. We use the **INT** type for vertex ids. If you want to work with a graph with more vertices than allowed by the range of values given by the **INT** type, you can use **BIGINT** at the cost of increased storage requirements.

{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

edges = spark.read.csv(path=filepath, sep=' ', comment='#', \
    schema='src INT, dst INT')
{% endhighlight %}

GraphFrames also requires a DataFrame with a column named **id** containing a row for each vertex in the graph. This DataFrame can be efficiently created by stacking the two columns of the **edges** DataFrame using the **union** method and eliminating duplicated rows using the **distinct** method. The DataFrame for each column is obtained using the **select** method, which takes one or more columns given by their name or a **Column** object. We use both approaches, the **Column**-based approach for source vertices, which allows to easily rename the column with the **alias** method, and the name-based approach for destination vertices, since the **union** method retains only the column name of the DataFrame that calls it.

{% highlight python %}
vertices = edges.select(edges['src'].alias('id')) \
    .union(edges.select('dst')) \
    .distinct()
{% endhighlight %}

Finally, the GraphFrame object is created by passing the DataFrames of vertices and edges as parameters.

{% highlight python %}
from graphframes import GraphFrame
graph = GraphFrame(vertices, edges)
{% endhighlight %}

The GraphFrame **graph** provides convenient properties and methods for working with graphs, such as obtaining vertex degrees or running the shortest path algorithm in the graph. To exemplify its use, a popular graph metric is implemented using the GraphFrame data structure in the following section.

## Implementing the Degree Assortativity Coefficient

The assortativity coefficient is a graph metric extensively used in network analysis that measures to what extent the vertices in a network are connected with similar vertices. Despite similarity can be defined according to different characteristics of vertices, the most commonly used feature in the context of assortativity is vertex degree. In this example, I'm going to show you how to implement the degree assortativity coefficient considering the graph as directed and using vertex out-degree (number of outgoing edges), but implementing the metric using in-degree (number of incoming edges) or for undirected graphs is straightforward given the following description. 

The degree assortativity coefficient is defined as the Pearson correlation coefficient of the degree between pairs of linked vertices. Since the DataFrame structure implements the Pearson correlation coefficient, we just have to build a DataFrame containing the degrees of all linked vertices.

The property **outDegrees** of the GraphFrame returns a DataFrame with a row for each vertex including its unique identifier in a column **id** and its out-degree in a column **outDegree**. We use **outDegrees** to build two DataFrames containing the out-degree, one for source vertices and another for destination vertices, and rename their columns to avoid overlapping column names as required in the following steps.

{% highlight python %}
source_degrees = graph.outDegrees \
    .withColumnRenamed('id','src') \
    .withColumnRenamed('outDegree','src_degree')
    
destination_degrees = graph.outDegrees \
    .withColumnRenamed('id','dst') \
    .withColumnRenamed('outDegree','dst_degree')
{% endhighlight %}

The property **edges** of the GraphFrame returns a DataFrame with a row for each edge containing the id of the source vertex and the target vertex in the columns **src** and **dst**, respectively. To build the DataFrame containing the out-degree of the source and the target vertices, the DataFrame of edges is combined with **source_degrees** using **src** as join column and with **destination_degrees** using **dst** as join column.

{% highlight python %}
combined_degrees = graph.edges \
    .join(source_degrees, on='src', how='left') \
    .join(destination_degrees, on='dst', how='left')
{% endhighlight %}

The **join** method of DataFrame takes another DataFrame and combine both by merging their rows with the same value in the column given by the **on** parameter. Since the DataFrame of edges is considered the left side of the join, we must specify the [type of the join](https://en.wikipedia.org/wiki/Join_(SQL)) as *left* with the **how** parameter to retain the rows of the DataFrame of edges.

Finally, we obtain the degree assortativity coefficient by computing the correlation between the **src_degree** and the **dst_degree** columns of the **combined_degrees** DataFrame.

{% highlight python %}
da_coeff = combined_degrees.corr('src_degree', 'dst_degree')
{% endhighlight %}

We have seen how easy it is to implement the degree assortativity coefficient. GraphFrame is also well suited for iterative graph algorithms, as we will see in the following session.

## Conclusions
We just learned how to load a graph and perform some basic analytics. I will show how to implement iterative algorithms using GraphFrames in future posts. I hope you enjoyed it!