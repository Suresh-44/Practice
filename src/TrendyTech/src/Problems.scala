object Problems extends App{


  def hof1(x:Int,y:Int,f:(Int,Int)=>Int) = {
    f(x,y)
  }

  val x1 = hof1(6,3,(x,y)=>x*x)
 // println(x1)

  val x2 = List(1,2,3,4,5)
 // println(x2.head)
  val x3 = x2.tail
  //println(x3)

 // println(x2.reverse)

  //println(x2(0))

 // println(x2.sum)

  val x4 = x3++x2
  //println(x4)

  val ran = 1 to 9
  //println(ran)

  abstract class xx{
    def eat
  }

  class x11 extends xx{
    def eat = println("jss;lfv")

  }

  val z = new x11
  z.eat


  case class w1() {
    def eat = println("eat")
  }

  class w2 extends w1{
    def eat(a:Int) = a*2
  }

  val w3 = new w2
  //println(w3.eat(4))

  println(w1.hashCode())
  println(w1.toString())




























   }







