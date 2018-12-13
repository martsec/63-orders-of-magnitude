


val ds = Seq((1, "sdw", "sre", BigDecimal("232.332323")), (2,null,"d",null)).toDF("id","str1","str2","decimal")

ds.select(concat(cols:_*).as("normal"), concat_ws("",cols:_*).as("ws")).show(false)