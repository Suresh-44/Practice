import scala.math.pow

object armstrong extends App {
  var n = 153
  var n1 = 153
  var n2 = n

  var count = 0
  var sum:Double = 0
  var r = 0
  while (n>=1){
    count+=1
    n = n/10
  }
  println(count)

  while (n1>=1){
    r = n1%10
    n1=  n1/10
    sum = sum+pow(r,count)
  }
  println(sum)
  println(sum==n2)
  //println("10".toInt)


}
