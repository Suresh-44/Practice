import java.util.regex.Pattern

import scala.util.matching.Regex

object Regex extends App{

  val p = Pattern.compile( "Suresh")
  val m = p.matcher(("my name is Suresh"))

  val x1 = m.start()
  println(x)

  val x = new Regex("Nidhi")
  val myself = "My name is Nidhi Singh."

  // replaces first match with the
  // String given below
  //rintln(x replaceFirstIn(myself, "Rahul"))

}
