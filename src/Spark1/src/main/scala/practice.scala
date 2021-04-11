
class Person(name:String,age:Int)
object practice extends App{
  val person = new Person("suri",27)

  val x = 3
  val y = if (x<5) 10 else 10.0
 // println(y)

  var x1  = 0
  while (x1<10) {
   // println(x1)
    x1+=1
  }

  val x2 = List(1,2,3,4,5,6,7)
  for (i <- x2){
    //println(i)
  }

  def x22(x:Int,y:Int=1)={
    x*y
  }

  //println(x22(3))

  def squareIt(x:Int)={
    x*x
  }

  def cubeIt(x:Int)={
    x*x*x
  }

  def s(x:Int,f:Int=>Int)={
    f(x)
  }

  //println(s(3,squareIt))
  //println(s(5,cubeIt))

 // println(s(6,x=>x*x*x))

  def v(s:String*)={
    for (i<-s){
      println(i)
    }
  }

  //v("a","q","s","z","x")

  def func: Unit ={
    throw new Exception
  }
  //func

  def gas(n:Int):Option[String]={
    if (n>=0) Some("aaaaaaa")
    else None
  }

  def printResult(num:Int)={
    gas(num) match {
      case Some(st)=>println(st)
      case None => println("No String")

    }
  }
 // printResult(4)

  val random = new scala.util.Random
  var start = 1
  val x22 =1
  val end = 60
  while (start<60) {
  //  println(x22+random.nextInt(end))
    start+=1
  }

  case class Address(city:String,state:String,zip:String)
  case class User(email:String,password:String){
    var fn:Option[String] = None
    var ln:Option[String] = None
    var address:Option[Address] = None
  }

  val user =  User("suri@gmail.com","1234")
  //println(user.fn.getOrElse("not assigned"))
  user.fn = Some("aaaa")
  //println(user.fn.getOrElse("not assigned"))
  user.ln = Some("ssss")
  user.address = Some(Address("q","w","e"))
  //println(user.address.getOrElse())
  val x33 = user.address
  //println(x33.getOrElse())

  def sq1(x:Int)={
    x*x
  }

  def hof1(x:Int,f:Int=>Int)={
    f(x)+f(x)
  }

  //println(hof1(3,sq1))



















}
