import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AgileEngineProject extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName(("AgileEngine")).master("local[*]")
    .getOrCreate()

  val df = spark.read.parquet("C:\\Users\\suresh\\Desktop\\Spark\\AgileEngineProject\\parquet\\")
  df.createOrReplaceTempView("test")
  //df.show(20,false)
  import spark.implicits._

  val df_timestamp = spark.sql("select from_unixtime(epochMillis/1000,'yyyy-MM-dd HH:mm:ss') as timestamp_2, * from test")
  //df_timestamp.show(2000, false)
  df_timestamp.createOrReplaceTempView("MainTable")

  println("*********** question 1 ********************")

  spark.sql("select count(distinct(navigation.navcode)),max(timestamp_2),min(timestamp_2) from maintable where mmsi=201006210").show(1000,false)

  val lead_df = spark.sql("select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by mmsi) as id from  (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER (partition by mmsi ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( partition by mmsi ORDER BY timestamp_2) as tempcolumn2 from maintable) temp1 where temp1.navcode !=temp1.tempcolumn2 order by mmsi")
 // lead_df.show(1000)
  lead_df.createOrReplaceTempView("leaddf")
  val lag_df = spark.sql("select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by mmsi) as id from  (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER (partition by mmsi ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( partition by mmsi ORDER BY timestamp_2) as tempcolumn2 from maintable) temp1 where temp1.navcode !=temp1.tempcolumn order by mmsi ")
 // lag_df.show(1000)
  lag_df.createOrReplaceTempView("lagdf")

  val df_q3 = spark.sql("select leaddf.mmsi,leaddf.timestamp as initial,lagdf.timestamp as final,leaddf.navcode,leaddf.navdesc,(unix_timestamp(lagdf.timestamp)-unix_timestamp(leaddf.timestamp))/3600  as lead_difference,(lagdf.epochMillis-leaddf.epochMillis)/1000 as leaddd   from lagdf join leaddf on lagdf.id = leaddf.id order by mmsi")
  df_q3.createOrReplaceTempView("q3")
 // df_q3.show(100,false)
  val df_q9 = spark.sql("select * from (select leaddf.mmsi,leaddf.timestamp as initial,lagdf.timestamp as final,leaddf.navcode,leaddf.navdesc,(unix_timestamp(lagdf.timestamp)-unix_timestamp(leaddf.timestamp))/3600  as lead_difference,(lagdf.epochMillis-leaddf.epochMillis)/1000 as leaddd   from lagdf join leaddf on lagdf.id = leaddf.id order by mmsi) temp order by lead_difference desc limit 10")
  df_q9.createOrReplaceTempView("q9")
  //df_q9.show(100,false)

  val df_q9P = spark.sql("select avg(lead_difference) as ld from q9")
  df_q9P.createOrReplaceTempView("q9p")

  println("******************* q10 ****************************************")
  val df_q10 = spark.sql("select q3.mmsi,q3.initial,q3.final,q3.navcode,q3.navdesc,if(q3.lead_difference>q9p.ld,'increased','decreased') as state from q3,q9p ")
  //df_q10.show(100,false)
  //spark.sql("select mmsi,count(*), sum(lead_difference) as sum, navdesc from (select leaddf.mmsi,leaddf.timestamp as initial,lagdf.timestamp as final,leaddf.navcode,leaddf.navdesc,(unix_timestamp(lagdf.timestamp)-unix_timestamp(leaddf.timestamp))/3600  as lead_difference,(lagdf.epochMillis-leaddf.epochMillis)/1000 as leaddd from lagdf join leaddf on lagdf.id = leaddf.id order by mmsi) temp where navdesc = 'at anchor' group by mmsi,navdesc ").show(false)

  println("*********** q11 ******************************")

  spark.sql("select mmsi,sum(lead_difference) as dwell from q3 where navdesc='At Anchor' or navdesc='Moored' group by mmsi order by mmsi").show(100,false)


  val df1 = spark.sql("select max(timestamp_2), min(timestamp_2) from MainTable")
  //spark.sql( "select sum(if(vesseldetails.typecode=0,1,0)) as zc_heading,,sum(if(navigation.courseOverGround=0,1,0)) as zc_COG,sum(if(navigation.navcode=0,1,0)) as zc_navcode,count(navigation.navcode) as count,count(distinct(navigation.navcode)) as distCount from maintable ").show()
  df1.show(false)
  spark.sql("select vesseldetails from maintable ").show(100,false)

  spark.sql("select sum(if(vesseldetails.typeCode=0,1,0)) as zc_typecode from maintable ").show()

  spark.sql("select sum(if(vesseldetails.draught=0.0,1,0)) as zc_draught from maintable ").show()

  spark.sql("select sum(if(vesseldetails.length=0,1,0)) as zc_length from maintable ").show()

  spark.sql("select sum(if(vesseldetails.width=0,1,0)) as zc_width from maintable ").show()

  spark.sql("select sum(if(mmsi=0,1,0)) as zc_mmsi from maintable ").show()

  spark.sql("select sum(if(imo=0,1,0)) as zc_imo from maintable ").show()

  spark.sql("select sum(if(callSign='0000000',1,0)) as zc_callsign from maintable ").show()

  spark.sql("select count(*) as zc_callsign from maintable where callsign like '0%0%0'or callsign like '0' ").show()

  spark.sql("select count(*) as null_callsign from maintable where callsign is null ").show()

  spark.sql("select count(*) as null_destination from maintable where destination is null ").show()

  spark.sql("select count(*) as null_imo from maintable where imo is null ").show()

  spark.sql("select count(*) as null_navdesc from maintable where navigation.navdesc Like 'unknown' ").show()

  spark.sql("select sum(if(cargodetails is null,1,0)) as null_cargodetails from maintable ").show()

  spark.sql("select sum(if(port.latitude=0,1,0)) as zc_port_latitude,sum(if(port.longitude=0,1,0)) as zc_port_longitude  from maintable ").show()

  spark.sql("select mmsi,vesselDetails.name as name,vesselDetails.typename as typename,vesselDetails.typecode as typeCode,vesselDetails.draught as draught,vesselDetails.length as length,vesselDetails.width as width from maintable where vesseldetails.typeCode=0").show(100,false)

  spark.sql("select navigation.navcode,navigation.navdesc,count(*) as frequency from maintable group by navigation.navcode,navigation.navdesc order by frequency desc").show(100, false)

  spark.sql("select navigation.navcode,navigation.navdesc,count(*) as frequency from maintable where mmsi = 205792000 group by navigation.navcode,navigation.navdesc order by frequency").show(100, false)

  spark.sql("select navigation.navcode,navigation.navdesc from maintable where mmsi = 205792000 ").show(100, false)

  spark.sql(" select mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 205792000 ").show(200,false)

  val df_lead = spark.sql(" select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by timestamp_2) as id from (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 205792000) temp1 where temp1.navcode !=temp1.tempcolumn")
  df_lead.createOrReplaceTempView("lead")
  val df_lag = spark.sql(" select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by timestamp_2) as id from (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 205792000) temp1 where temp1.navcode !=temp1.tempcolumn2 ")
  df_lag.createOrReplaceTempView("lag")

  spark.sql("select lead.mmsi,lead.timestamp,lead.navcode,lead.navdesc,(unix_timestamp(lead.timestamp)-unix_timestamp(lag.timestamp))*1000  as lead_difference,(lead.epochMillis-lag.epochMillis) as leaddd   from lag join lead on lag.id = lead.id").show(false)

  spark.sql("select count(*) from maintable where mmsi = 205792000").show()

  println("***********************   mmsi = 413970021  ************************ ")


  spark.sql("select navigation.navcode,navigation.navdesc,count(*) as frequency from maintable where mmsi = 413970021 group by navigation.navcode,navigation.navdesc order by frequency").show(100, false)

  spark.sql("select navigation.navcode,navigation.navdesc from maintable where mmsi = 413970021 ").show(100, false)

  spark.sql(" select mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 413970021 ").show(200,false)

  val df_lead_413970021 = spark.sql(" select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by timestamp_2) as id from (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 413970021) temp1 where temp1.navcode !=temp1.tempcolumn")
  df_lead_413970021.createOrReplaceTempView("lead")
  val df_lag_413970021 = spark.sql(" select epochMillis,mmsi,timestamp_2 as timestamp, navcode, navdesc,row_number() over(order by timestamp_2) as id from (select epochMillis,mmsi,timestamp_2,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn,LAG(navigation.navcode,1,-100) OVER ( ORDER BY timestamp_2) as tempcolumn2 from maintable where mmsi = 413970021) temp1 where temp1.navcode !=temp1.tempcolumn2 ")
  df_lag_413970021.createOrReplaceTempView("lag")

  spark.sql("select lead.mmsi,lead.timestamp,lead.navcode,lead.navdesc,(unix_timestamp(lead.timestamp)-unix_timestamp(lag.timestamp))*1000  as lead_difference,(lead.epochMillis-lag.epochMillis) as leaddd   from lag join lead on lag.id = lead.id").show(false)

  spark.sql("select * from maintable where mmsi = 413970021").show(100,false)






  df.printSchema()

}
