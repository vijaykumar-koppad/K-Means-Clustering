# K-Means Clustering

This project involves implementation of the k-means clustering algorithm using iterative map-reduce using Hadoop and hbase.

Firstly create a schema within  Hbase to store the data, and then implement clustering algorithm that can cluster dataset into k groups by using the map and reduce primitives. Here k-means algorithms is used for clustering.


The dataset is from UCI machine learning repository. Basically, it is collected to analysis  the energy	efficiency of building with different shapes. There are totally 768 data points, and each of these points has 8 features and 2 responses.

DataSet	Description can be found [here](http://archive.ics.uci.edu/ml/datasets/Energy+efficiency).

Data	Schema:

							Energy			-->	table	name
															    
                            Area											      |    Property   -->     column family
                            
							X1,	X5,	X6,	Y1,	Y2	 |	 X2, X3, X4, X7, X8 

Key							of	each	data	point	is	 rowi,	 like	 row1,	 row2.... row10000.... 

1) program accepts 2  parameters:	

   1. hdfs path of data input	 
   2. Number of cluster to calculate.
													
2) program firstly deletes the	HTables	to use, create data table (for storing data) and center table for result.
3) It iinitialise first	 K initial	data	points based	  on   required	     schema	in    the center table.
4) It stores these data into the Hbase  through using a map-reduce    job.
5) Implements an iterative map reduce to cluster these 768 data points into k group with the condition	that data   points in the same group  are  as similar as possible and points in different group	are as different as possible.
6) Generates the center	points of these k group data subset into a HTable called center.
