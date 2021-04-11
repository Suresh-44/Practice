import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoinsInterview extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("joinsinterview")).master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val customersdf = Seq((3002,"Nick Rimando", "New York",100,5001),
    (3007,"Brad Davis","New York",200,5001),
    (3005 , "Graham Zusi", "California",200,5002),
  (3008 , "Julian Green","London",300,5002),
  (3004 ,"Fabian Johnson" ,"Paris",300,5006),
  (3009 , "Geoff Cameron" ,"Berlin",100 ,5003),
  (3003 , "Jozy Altidor", "Moscow",200,5007),
  (3001, "Brad Guzan","London",0,5005)).toDF( "customer_id","cust_name","city","grade","salesman_id")

  customersdf.show()

  val Salesmandf = Seq((5001,"James Hoog","New York",0.15),
    (5002, "Nail Knite","Paris",0.13),
    (5005,"Pit Alex","London",0.11),
    (5006,"Mc Lyon","Paris",0.14),
  (5007,"Paul Adam","Rome",0.13),
  (5003,"Lauson Hen","San Jose",0.12)).toDF("salesman_id","name","city","commission")

  Salesmandf.show()

  val Ordersdf = Seq((70001,150.5,"2012-10-05",3005,5002),
    (70009,270.65,"2012-09-10",3001 ,5005),
    (70002,65.26,"2012-10-05",3002,5001),
  (70004,110.5,"2012-08-17",3009,5003),
  (70007,948.5,"2012-09-10",3005,5002),
  (70005,2400.6 ,"2012-07-27",3007,5001),
  (70008,5760.0,"2012-09-10",3002,5001),
  (70010,1983.43, "2012-10-10",3004,5006),
  (70003,2480.4, "2012-10-10",3009,5003),
  (70012,250.45,"2012-06-27",3008,5002),
  (70011,75.29,"2012-08-17",3003,5007),
  (70013,3045.6,"2012-04-25",3002,5001)).toDF("ord_no","purch_amt","ord_date","customer_id","salesman_id")


  Ordersdf.show()

  customersdf.createOrReplaceTempView("customers")
  Salesmandf.createOrReplaceTempView("sales")
  Ordersdf.createOrReplaceTempView("orders")

  /* 14. Write a SQL statement to make a list for the salesmen who either work for one or more
  customers or yet to join any of the customer.
  The customer may have placed, either one or more orders on or above order amount 2000
  and must have a grade, or he may not have placed any order to the associated supplier.
   */

  spark.sql(
    """select * from sales s join customers c on s.salesman_id = c.salesman_id left join
      | orders o on c.customer_id = o.customer_id  where o.purch_amt>2000 and grade!=0
      |""".stripMargin).show()

  /*15. Write a SQL statement to make a report with customer name, city,
  order no. order date, purchase amount for those customers from the existing list
   who placed one or more orders or
  which order(s) have been placed by the customer who is not on the list
  * */

  spark.sql(
    """
      |select c.cust_name,c.city,o.ord_no,o.ord_date,o.purch_amt from customers c
      | right join orders o on c.customer_id = o.customer_id
      |""".stripMargin).show()

  // in  Q15 solution we can do FULL OUTER JOIN

  /*16. Write a SQL statement to make a report with customer name, city,
  order no. order date, purchase amount for only those customers on the list
  who must have a grade and placed one or more orders or which order(s)
  have been placed by the customer who is neither in the list
   */

  spark.sql(
    """
      |select c.cust_name,c.city,o.ord_no,o.ord_date,o.purch_amt from customers c
      | right join orders o on c.customer_id = o.customer_id
      |""".stripMargin).show()


  /* 17. Write a SQL statement to make a cartesian product between salesman and
   customer i.e. each salesman will appear for all customer and vice versa.
  */

  spark.sql(
    """
      |select * from customers cross join orders
      |""".stripMargin).show()

  /*18. Write a SQL statement to make a cartesian product between salesman
  and customer i.e. each salesman will appear
  for all customer and vice versa for that salesman who belongs to a city.*/

  spark.sql(
    """
      |SELECT *
      |FROM sales a
      |CROSS JOIN customers b
      |WHERE a.city IS NOT NULL
      |""".stripMargin).show()

  /*19. Write a SQL statement to make a cartesian product between salesman and
  customer i.e. each salesman will appear for all customer and vice versa for those salesmen
  who belongs to a city and the customers who must have a grade.
   */

  spark.sql(
    """
      |SELECT *
      |FROM sales a
      |CROSS JOIN customers b
      |WHERE a.city IS NOT NULL and b.grade!=0
      |""".stripMargin).show()

  /*20. Write a SQL statement to make a cartesian product between salesman and customer i.e.
  each salesman will appear for all customer and vice versa for those salesmen
  who must belong a city which is not the same
   as his customer and the customers should have an own grade. */

  spark.sql(
    """
      |SELECT *
      |FROM sales a
      |CROSS JOIN customers b
      |WHERE a.city IS NOT NULL and b.grade!=0
      | and a.city!=b.city
      |""".stripMargin).show()

  /*21. Write a SQL query to display all the data from the item_mast,
  including all the data for each item's producer company.*/














}
