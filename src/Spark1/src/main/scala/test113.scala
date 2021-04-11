object test113 extends App {

  def getA(num:Int):Option[String] = {
    if (num>=0) Some("A positive number")
    else None
  }

  def printResult(num:Int)={
    getA(num) match {
      case Some("A positive number") => println("A positive number")
      case None => println("No String")
    }
  }

//  printResult(4)

  val x = for (i <- 1 to 10) yield {
    i*i
  }

  //println(x)

  def F(x:Int) = {
    x match {
      case a if a >0 => println(s"$a is +ve number")
      case b if b<0  => println(s"$b is -ve number")
      case c => println(s"$c is neither +ve nor -ve")
    }
  }

 // F(0)

  def F(x:Option[Int]) = {
    x match {
      case Some(i) if i%2==0 => println(i)
      case None => println("none")
      case _ => println("something else")
    }
  }

 // F(Option(10))

  val x1 = 5
  val y = 6
  val z = x1>y match {
    case true => x1
    case false => y
  }

  //println(z)


  for (i <- 2 to 100){
    var count = 0
    for (j <- 2 until i){
      if (i%j==0) count+=1
    }
    if (count==0) println(s"$i is a prime number")
    
  }





}
