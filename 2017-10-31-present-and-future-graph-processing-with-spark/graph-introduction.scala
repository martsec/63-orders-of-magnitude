val raw = """{"source": "Microsoft", "target": "Amazon", "type": "licensing"}
  {"source": "Microsoft", "target": "HTC", "type": "licensing"}
  {"source": "Samsung", "target": "Apple", "type": "suit"}
  {"source": "Motorola", "target": "Apple", "type": "suit"}
  {"source": "Nokia", "target": "Apple", "type": "resolved"}
  {"source": "HTC", "target": "Apple", "type": "suit"}
  {"source": "Kodak", "target": "Apple", "type": "suit"}
  {"source": "Microsoft", "target": "Barnes & Noble", "type": "suit"}
  {"source": "Microsoft", "target": "Foxconn", "type": "suit"}
  {"source": "Oracle", "target": "Google", "type": "suit"}
  {"source": "Apple", "target": "HTC", "type": "suit"}
  {"source": "Microsoft", "target": "Inventec", "type": "suit"}
  {"source": "Samsung", "target": "Kodak", "type": "resolved"}
  {"source": "LG", "target": "Kodak", "type": "resolved"}
  {"source": "RIM", "target": "Kodak", "type": "suit"}
  {"source": "Sony", "target": "LG", "type": "suit"}
  {"source": "Kodak", "target": "LG", "type": "resolved"}
  {"source": "Apple", "target": "Nokia", "type": "resolved"}
  {"source": "Qualcomm", "target": "Nokia", "type": "resolved"}
  {"source": "Apple", "target": "Motorola", "type": "suit"}
  {"source": "Microsoft", "target": "Motorola", "type": "suit"}
  {"source": "Motorola", "target": "Microsoft", "type": "suit"}
  {"source": "Huawei", "target": "ZTE", "type": "suit"}
  {"source": "Ericsson", "target": "ZTE", "type": "suit"}
  {"source": "Kodak", "target": "Samsung", "type": "resolved"}
  {"source": "Apple", "target": "Samsung", "type": "suit"}
  {"source": "Kodak", "target": "RIM", "type": "suit"}
  {"source": "Nokia", "target": "Qualcomm", "type": "suit"}""".split("\n").map(_.trim)
val temporalDataset = spark.createDataset(raw)
val suitsDF = spark.read.json(temporalDataset)
val companiesDF = suitsDF.select($"source".as("name")).
   union(suitsDF.select($"target".as("name"))).
   distinct

suitsDF.cache()
companiesDF.cache()


/** 
  * GraphX
  */
import org.apache.spark.graphx._

val t1GX = System.currentTimeMillis()
// Handy function to compute the ids
def id(name: String): Long = name.hashCode.toLong
// Create an RDD for the nodes
val companiesRDD: RDD[(VertexId, (String, String))] = companiesDF.rdd.map{row => 
    val name = row.getString(0)
    (id(name), (name, ""))
  }
//Create an RDD for the edges, using the class Edges
val suitsRDD: RDD[Edge[String]] = suitsDF.rdd.map{row => 
    val source = row.getString(0)
    val destination = row.getString(1)
    Edge(id(source), id(destination), row.getString(2))
  }
// Define a default user in case there are relationships with missing nodes
val defaultCompany = ("EvilCorp", "Mising")
// Aaaaaaaaaaaaaand build it!
val graph = Graph(companiesRDD, suitsRDD, defaultCompany)
graph.cache
// Compute connected components 
val cc = graph.connectedComponents().vertices

// Obtain the name of the companies and split them according to their groups. And format a bit
val ccByCompany = companiesRDD.join(cc).map {
  case (id, (company, cc)) => (cc, company._1)
}.groupBy(_._1).map(_._2.map(_._2).mkString(", "))

// Print the result
val finalGX = ccByCompany.collect().mkString("\n")
val t2GX = System.currentTimeMillis()

/** 
  * GraphFrame
  */
import org.graphframes._
import org.apache.spark.sql.functions.collect_list

val t1GF = System.currentTimeMillis()
val companiesGF = companiesDF.select($"name".as("id"))
val suitsGF = suitsDF.select($"source".as("src"), $"target".as("dst"), $"type")
val g = GraphFrame(companiesGF, suitsGF)
g.persist
// Required to run the algorithm
sc.setCheckpointDir("/tmp")

val result = g.connectedComponents.run()
val companiesByGFCC = result.select("id", "component").
    groupBy("component").
    agg(collect_list("id").as("companylist")).
    select("companylist").
    map(r => r.getSeq(0).mkString(", "))
val finalGF = companiesByGFCC.collect.mkString("\n")
val t2GF = System.currentTimeMillis()

/** 
  * GraphFrame + GraphX algorithm
  */
val t1GFGX = System.currentTimeMillis()
val companiesGFGX = companiesDF.select($"name".as("id"))
val suitsGFGX = suitsDF.select($"source".as("src"), $"target".as("dst"), $"type")
val gGX = GraphFrame(companiesGFGX, suitsGFGX)
gGX.persist
val resultGX = gGX.connectedComponents.setAlgorithm("graphx").run()
val companiesByGFCCGX = resultGX.select("id", "component").
    groupBy("component").
    agg(collect_list("id").as("companylist")).
    select("companylist").
    map(r => r.getSeq(0).mkString(", "))
val finalGFGX = companiesByGFCCGX.collect.mkString("\n")
val t2GFGX = System.currentTimeMillis()


println("GraphX:               %3.2f s".format((t2GX-t1GX)/1000.0))
println("GraphFrames native:   %3.2f s".format((t2GF-t1GF)/1000.0))
println("GraphFrames w/GraphX: %3.2f s".format((t2GFGX-t1GFGX)/1000.0))