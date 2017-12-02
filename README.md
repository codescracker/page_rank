This project is the implementation of well-known PageRank algorithm based on Hadoop. The mathematical model behind the PageRank algorithm is Markov chain. This model requires a lot of iterations of linear algebra computation. The difficult part of implementation of PageRank is the complexity of matrix which can be very high dimensional, thus cause problems for CPU performance and Memory.

**The Pros of Hadoop for PageRnak implementation:**   
The MapReduce model in Hadoop enable to decompose high dimension matrix into key-value pair pattern, and parallel the computation process leveraging power of nodes in cluster. Thus, it can overcome the bottleneck of CPU and Memory. And finish the very high dimensional matrix computation.

**The Cons of Hadoop for PageRank implementation:**<br />
Hadoop has to write the middle result of computation into disk, which is very slow.  For the algorithms like PageRank that include iterative computation, I/O will become the bottleneck of performance. Later, I will push finish the implementation of PageRank algorithm based on Spark, which can store the middle results in memory, and theoretically overcome the drawback of Hadoop in this case.  



