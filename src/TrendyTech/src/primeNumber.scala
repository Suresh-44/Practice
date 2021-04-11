import scala.util.control.Breaks._

object primeNumber extends App {

  // Prime Numbers between 1 to 100

    for (i <- 3 to 100) {
      var count = 0
        for (j <- 2 until i) {
          if (i % j == 0) {
            count += 1
          }
        }
        if (count>=1) {println(s"$i not a prime")}
        else println(s"$i is a prime")

    }

}
