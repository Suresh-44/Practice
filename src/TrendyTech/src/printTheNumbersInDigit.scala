import scala.math.pow


object printTheNumbersInDigit extends App {

  var n = 123
  // println(pow(3,3))
  var sum =0
  var r = 0

  while (n>10) {
      r = n%10
      n = n /10
      sum= sum+r*r*r
  }
println(sum)

}
