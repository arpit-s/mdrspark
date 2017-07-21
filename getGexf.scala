def toGexf[VD,ED](g:Graph[VD,ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "    <nodes>\n" +
    g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
                        v._2 + "\" />\n").collect.mkString +
    "    </nodes>\n" +
    "    <edges>\n" +
    g.edges.map(e => "      <edge source=\"" + e.srcId +
                     "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
                     "\" />\n").collect.mkString +
    "    </edges>\n" +
    "  </graph>\n" +
    "</gexf>"

val pw = new java.io.PrintWriter("myGraph.gexf")
pw.write(toGexf(gx))
pw.close


//from local
scp hdpmdrclusterssh@mdrspark-ssh.azurehdinsight.net:/home/hdpmdrclusterssh/myGraph.gexf /home/arpit/
