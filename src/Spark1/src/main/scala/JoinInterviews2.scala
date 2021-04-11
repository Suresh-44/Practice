import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoinInterviews2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("joinsinterview")).master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val companydf = Seq((11,"Samsung"),
    (12,"iBall"),
    (13,"Epsion"),
    (14,"Zebronics"),
    (15,"Asus"),
    (16, "Frontech")).toDF("com_id","com_name")

  companydf.show()


  val itemsdf = Seq((101,"Mother Board",3200,15),
    (102 ,"Key Board",450,16),
    (103, "ZIP drive", 250 ,14),
    (104, "Speaker", 550, 16),
    (105, "Monitor",5000, 11),
  (106,"DVD driver",900, 12),
    (107,"CD drive", 800,12),
    (108 ,"Printer", 2600,13),
    (109, "Refill cartridge", 350, 13),
    (110, "Mouse",250,12)).toDF("pro_id","pro_name","pro_price","pro_com")

  itemsdf.show()

  companydf.createOrReplaceTempView("company")
  itemsdf.createOrReplaceTempView("items")

  /*21. Write a SQL query to display all the data from the item_mast,
  including all the data for each item's producer company.*/

 spark.sql(
   """ select * from items i left join company c
     | on i.pro_com = c.com_id
     |""".stripMargin).show()


  /* 22. Write a SQL query to display the item name, price,
  and company name of all the products.
   */

 /* spark.sql(
    """
      |
      |""".stripMargin).show() */

  /* 23. Write a SQL query to display the average price
  of items of each company, showing the name of the company.
   */

  spark.sql(
    """ select c.com_name,avg(i.pro_price) from company c join items i
      | on c.com_id = i.pro_com
      | group by c.com_name
      |""".stripMargin).show()

  /* 24. Write a SQL query to display the names of the company whose products
   have an average price larger than or equal to Rs. 350.
    */













}
