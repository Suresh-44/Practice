import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer


object SQLInterview extends Serializable {

  def f1(n:Int):ListBuffer[Int] ={
    var a = new ListBuffer[Int]()
    var i = 0
    while (i<n){
      a +=1
      i+=1
    }
    a
  }

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName(("joinsinterview")).master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //  1.  Delete duplicate data from table so that only first data remains constant
    //Managers

    val managersDF = Seq((1, "Harpreet", 20000),
      (2, "Ravi", 30000),
      (3, "Vinay", 10000),
      (4, "Ravi", 30000),
      (5, "Harpreet", 20000),
      (6, "Vinay", 10000),
      (7, "Rajeev", 40000),
      (8, "Vinay", 10000),
      (9, "Ravi", 30000)).toDF("id", "name", "salary")

    //managersDF.printSchema()
    //managersDF.show()

    managersDF.createOrReplaceTempView("managers")

    spark.sql(
      """ select id,name,salary from
        | (select id,name,salary, dense_rank() over(partition by name order by id ) as dup from managers) r
        | where r.dup = 1 order by id
        |""".stripMargin)

    // 2.  Find the Name of Employees where First Name, Second Name,
    // and Last Name is given in the table. Some Name is missing such as
    // First Name, Second Name and maybe Last Name.
    // Here we will use COALESCE() function which will return first Non Null values.
    //Employees

    val employeesDF = Seq((1, "Har", "preet", "Singh", 30000),
      (2, "Ashu", null, "Rana", 50000),
      (3, null, "Vinay", "Thakur", 40000),
      (4, null, "Vinay", null, 10000),
      (5, null, null, "Rajveer", 60000),
      (6, "Manjeet", "Singh", null, 60000)).toDF("id", "FName", "SName", "LName", "Salary")

    //employeesDF.show()
    employeesDF.createOrReplaceTempView("employees")

    spark.sql(
      """
        | SELECT ID, COALESCE(FName, SName, LName) as Name FROM employees
        |""".stripMargin)



    //  3.  Find the Emloyees who were hired in the Last n months
    //Finding the Employees who have been hire in the last n months.
    // Here we get desired output by using TIMESTAMPDIFF() mysql function
    //Employees

    val employessDF1 = Seq((1, "Rajveer", "Singh", "Male", 30000, "2017/11/05"),
      (2, "Manveer", "Singh", "Male", 50000, "2017/11/05"),
      (3, "Ashutosh", "Kumar", "Male", 40000, "2017/12/12"),
      (4, "Ankita", "Sharma", "Female", 45000, "2017/12/15"),
      (5, "Vijay", "Kumar", "Male", 50000, "2018/01/12"),
      (6, "Dilip", "Yadav", "Male", 25000, "2018/02/26"),
      (7, "Jayvijay", "Singh", "Male", 30000, "2018/02/18"),
      (8, "Reenu", "Kumari", "Female", 40000, "2017/09/19"),
      (9, "Ankit", "Verma", "Male", 25000, "2018/04/04"),
      (10, "Harpreet", "Singh", "Male", 50000, "2017/10/1")).toDF("id", "FName", "LName", "gender", "salary",
      "hiredate")

    //  employessDF1.show()
    employessDF1.createOrReplaceTempView(("emp1"))

    spark.sql(
      """ select
        | date_format(hiredate,"yyyy/MM/dd") from emp1
        |""".stripMargin)

    // 4. Transatcion_tbl Table has four columns CustID, TranID, TranAmt, and  TranDate.
    // User has to display all these fields
    // along with maximum TranAmt for each CustID and ratio of TranAmt
    // and maximum TranAmt for each transaction.


    val tranDF = Seq(
      (1001, 20001, 10000, "2020-04-25"),
      (1001, 20002, 15000, "2020-04-25"),
      (1001, 20003, 80000, "2020-04-25"),
      (1001, 28004, 20000, "2020-04-25"),
      (1002, 30001, 7000, "2020-04-25"),
      (1002, 30002, 15000, "2020-04-25"),
      (1002, 30003, 22000, "2020-04-25")).toDF("custid", "tranid", "tranamt", "trandate")

    //tranDF.show()
    tranDF.createOrReplaceTempView("tran")

    spark.sql(
      """ select *, (tranamt/maxamt) as ratio  from  (select *, max(tranamt) over(partition by custid order by custid) as maxamt from tran)
        |
        |
        |""".stripMargin)

    // 5. Student Table has three columns Student_Name, Total_Marks and Year.
    // User has to write a SQL query to display Student_Name, Total_Marks, Year,
    // Prev_Yr_Marks for those whose Total_Marks are greater than or equal to the previous year.


    val studentsDF = Seq(("Rahul", 90, 2010),
      ("Sanjay", 80, 2010),
      ("Mohan", 70, 2010),
      ("Rahul", 90, 2011),
      ("Sanjay", 85, 2011),
      ("Mohan", 65, 2011),
      ("Rahul", 80, 2012),
      ("Sanjay", 80, 2012),
      ("Mohan", 90, 2012)).toDF("studentName", "totalmarks", "year")

    //  studentsDF.show()

    studentsDF.createOrReplaceTempView("students")

    spark.sql(
      """ select * from (select *,lag(totalmarks,1,1000) over(partition by studentName order by year) as prevmarks from students) r
        | where r.totalmarks>=r.prevmarks
        |""".stripMargin)


    // 6. Emp_Details  Table has four columns EmpID, Gender, EmailID and DeptID.
    // User has to write a SQL query to derive another column called Email_List to display
    // all Emailid concatenated with semicolon associated with a each DEPT_ID  as shown below in output Table.

    val empemail = Seq(
      (1001, "M", "yyy@gmail.com", 104),
      (1002, "M", "zzz@gmail.com", 103),
      (1003, "F", "aaa@gmail.com", 102),
      (1004, "F", "ff@gmail.com", 104),
      (1005, "M", "ccc@gmail.com", 101),
      (1006, "M", "ddd@gmail.com", 100),
      (1007, "f", "e@gmail.com", 102),
      (1008, "M", "m@gmail.com", 102),
      (1009, "f", "ss@gmail.com", 100)
    ).toDF("empid", "gender", "emailid", "deptid")

    //empemail.show()

    empemail.createOrReplaceTempView("email")

    spark.sql(
      """ select deptid, concat_ws(';',collect_list(emailid))
        | from email
        | group by deptid
        | order by deptid
        |""".stripMargin)


    // 7. Order_Tbl has four columns namely ORDER_DAY, ORDER_ID, PRODUCT_ID, QUANTITY and PRICE
    //(a) Write a SQL to get all the products that got sold on both the days and the number of times the product is sold.
    //(b) (b) Write a SQL to get products that was ordered on 02-May-2015 but not on 01-May-2015

    val productsDF1 = Seq(
      ("2015-05-01", "ODR1", "PROD1", 5, 5),
      ("2015-05-01", "ODR2", "PROD2", 2, 10),
      ("2015-05-01", "ODR3", "PROD3", 10, 25),
      ("2015-05-01", "ODR4", "PROD1", 20, 5),
      ("2015-05-02", "ODR5", "PROD3", 5, 25),
      ("2015-05-02", "ODR6", "PROD4", 6, 20),
      ("2015-05-02", "ODR7", "PROD1", 2, 5),
      ("2015-05-02", "ODR8", "PROD5", 1, 50),
      ("2015-05-02", "ODR9", "PROD6", 2, 50),
      ("2015-05-02", "ODR10", "PROD2", 4, 10)).toDF("orderdate", "order_id", "product_id", "quantity", "price")

    //productsDF1.show()
    productsDF1.createOrReplaceTempView("prod")

    spark.sql(
      """ select product_id, size(collect_set(orderdate)) as size,count(*) as count
        | from prod
        | group by product_id
        | having size>=2
        |
        |""".stripMargin)

    spark.sql(
      """ select product_id from prod where orderdate = "2015-05-02" and product_id not in
        |  (select product_id from prod where orderdate = "2015-05-01")
        |
        |""".stripMargin)

    // a) Write a SQL to get the highest sold Products (Quantity*Price) on both the days

    spark.sql(
      """ select orderdate, product_id,quantity*price  from prod where (orderdate,quantity*price) in
        |   (select orderdate,max(quantity*price) as quantitysold from prod
        | group by orderdate)
        |""".stripMargin)

    //(b) Write a SQL to get all product's  total sales on 1st May and 2nd May adjacent to each other

    spark.sql(
      """ select t2.product_id,t1.sale01,t2.sale02 from  (select product_id,sum(quantity*price) as sale01 from prod
        | where orderdate = "2015-05-01"
        | group by product_id) t1 right join
        | (select product_id,sum(quantity*price) as sale02 from prod
        | where orderdate = "2015-05-02"
        | group by product_id) t2 on t1.product_id = t2.product_id
        |
        | order by t2.product_id
        |""".stripMargin)

    //(c) Write a SQL to get all products day wise, that was ordered more than once

    spark.sql(
      """ select orderdate,product_id,count(*) as cnt from prod
        |  group by orderdate,product_id
        |  having cnt>1
        |""".stripMargin)

    // 8.  INPUT :-  Order_Tbl has four columns namely ORDER_ID, PRODUCT_ID, QUANTITY and PRICE
    //Problem Statements :-
    //Write a SQL which will explode the above data into single unit level records as shown below

    val dupdf = Seq(
      ("odr1", "prd1", 5),
      ("odr2", "prd2", 1),
      ("odr3", "prd3", 3)
    ).toDF("orderid", "productid", "quantity")

    //dupdf.show()

    spark.udf.register("f",f1(_:Int):ListBuffer[Int])
    dupdf.createOrReplaceTempView("dup")


    spark.sql(
      """ select orderid,productid,explode(l) from (select orderid,productid,quantity,f(quantity) as l from dup) t
        |
        |""".stripMargin)


    // 9.  Employee Table has four columns namely EmpID , EmpName, Salary and DeptID
    //Write a SQL  to find all Employees who earn more than the average salary in their corresponding department.


    val empdf2 = Seq(
      (1001,"mark",60000,2),
      (1002,"antony",40000,2),
      (1003,"andrew",15000,1),
      (1004,"peter",35000,1),
      (1005,"john",55000,1),
      (1006,"albert",25000,3),
      (1007,"donald",35000,3)
    ).toDF("empid","employee","salary","deptid")

   // empdf2.show()

    empdf2.createOrReplaceTempView("emp2")

   /* spark.sql(
      """ select emp2.employee,emp2.deptid,emp2.salary,emp2.aveg from (select deptid,employee, salary from emp2 ) join (select deptid as d,avg(salary) as aveg  from emp2
        | group by d) t on emp2.deptid = t.d)
        |
        |""".stripMargin).show()  */


    /* 10. Team Table has two columns namely ID and TeamName and it contains 4 TeamsName.
       Problem Statements :- Write a SQL which will fetch
       total schedule of matches between each Team vs opposite team:
     */

    val teamdf = Seq(
      (1,"India"),
      (2,"Australia"),
      (3,"England"),
      (4,"NewZealand")
    ).toDF("id","country")

   // teamdf.show()

    teamdf.createOrReplaceTempView("teams")

    spark.sql(
      """ select t1.country,t2.country from teams t1 cross join teams t2
        |  where t1.id != t2.id and t1.id< t2.id
        |
        |""".stripMargin)

    /* 11.  Table Match_Result has three columns  namely Team_1, Team_2 and Result.
Problem Statements :- Write SQL to display total number
 of matches played, matches won, matches tied and matches lost for each team
     */


    /* 12. Write SQL to get the total Sales in year 1998,1999 and 2000 for all the products as shown below.
     */

    val saledf1 = Seq(
      (1,"Laptop",1998,2500),
      (2,"Laptop",1999,3600),
      (3,"Laptop",2000,4200),
      (4,"Keyword",1998,2300),
      (5,"Keyword",1999,4800),
      (6,"Keyword",2000,5000),
      (7,"Mouse",1998,6000),
      (8,"Mouse",1999,3400),
      (9,"Mouse",2000,4600)
    ).toDF("id","product","year","price")

    saledf1.show()

    saledf1.groupBy("year").pivot("year").sum("price")

    saledf1.createOrReplaceTempView("sale1")

 /*   spark.sql(
      """ select  group by year pivot(year) sum(price) from sale1
        |
        |""".stripMargin).show()  */










  }

}
