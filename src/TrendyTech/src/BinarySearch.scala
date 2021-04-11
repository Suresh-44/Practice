object BinarySearch extends App{
  val a = List(1,2,3,4,5)
  println(a.size/2)

  val k = 2

  def BinarySearch(a:List[Int],k:Int):Int= {
    if (a(0)==k) 0

    var mid = a.size / 2
    if (a(mid) == k) mid

    else if (a(mid) > k) {
      var x = BinarySearch(a.slice(0, mid), k)
      mid-x
    }
    else {
      var x =  BinarySearch(a.slice(mid, a.size), k)
      x+mid
    }

  }
  val x = BinarySearch(a,k)
  println(x)


}
