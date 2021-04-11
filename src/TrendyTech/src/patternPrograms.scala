object patternPrograms extends App{

  ////////////////////////////////
  for (i <- 1 to 5){
    for (j <- 1 to i){
      print(j)
      print(" ")
    }
    println()
  }
  ///////////////////////////////////
  for (i <- 1 to 5){
    for (j <- 1 to i){
      print(i)
      print(" ")
    }
    println()
  }
  ///////////////////////////////////
  for (i <- 1 to 5){
    for (j <- 1 to i){
      print("*")
      print(" ")
    }
    println()
  }
 ////////////////////////////////////
  val n = 10
  for (i <- 1 to n) {
    for (j <- 1 to i) {
      if (j==1 || i==n || j==i ){
        print("*")
        //print(" ")
      }else {print(" ")}
    }
    println()
  }

  ////////////////////////////////////






}
