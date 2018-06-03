---
title: "Implementing Graph Algorithms Using Graphframes for Apache Spark in Python"
date: 2018-06-03 00:00:00
categories: networks, graphs, big data
---

[GraphFrames](http://graphframes.github.io/) is a graph-parallel processing library for Apache Spark. In contrast to [GraphX](https://spark.apache.org/graphx/), which is built on top of the resilient distributed dataset (RDD), GraphFrames is based on DataFrames. The DataFrame is a data structure providing a higher level of abstraction and better performance compared to the RDD as result of defining a schema for the data in advance. Another advantage of GraphFrames is that it is currently available for Python, while GraphX is not [and probably won't be](https://issues.apache.org/jira/browse/SPARK-3789). 

In this post im going to describe how to implement...

## Implementing the Degree Assortativity Coefficient

The assortativity coefficient is a graph metric extensively used in network analysis that measures to what extent the nodes in a network are connected with similar nodes. Despite similarity can be defined according to different characteristics of nodes, the most commonly used feature in the context of assortativity is node degree. In this example, I'm going to show you how to implement the degree assortativity coefficient considering the graph as directed and using node out-degree (number of outgoing edges), but implementing the metric using in-degree (number of incoming edges) or for undirected graphs is straightforward given the following description. 

The degree assortativity coefficient is defined as the Pearson correlation coefficient of the degree between pairs of linked nodes. Since the DataFrame structure implements the Pearson correlation coefficient, we just have to build a DataFrame containing the degrees of all linked nodes.

The property **outDegrees** of the GraphFrame returns a DataFrame with a row for each node including its unique identifier in a column **id** and its out-degree in a column **outDegree**. We use **outDegrees** to build two DataFrames containing the out-degree, one for source nodes and another for destination nodes, and rename their columns to avoid overlapping column names as required in the following steps.

{% highlight python %}
source_degrees = graph.outDegrees \
    .withColumnRenamed('id','src') \
    .withColumnRenamed('outDegree','src_degree')
    
destination_degrees = graph.outDegrees \
    .withColumnRenamed('id','dst') \
    .withColumnRenamed('outDegree','dst_degree')
{% endhighlight %}

The property **edges** of the GraphFrame returns a DataFrame with a row for each edge containing the id of the source node and the target node in the columns **src** and **dst**, respectively. To build the DataFrame containing the out-degree of the source and the target nodes, the DataFrame of edges is combined with **source_degrees** using **src** as join column and with **destination_degrees** using **dst** as join column.

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