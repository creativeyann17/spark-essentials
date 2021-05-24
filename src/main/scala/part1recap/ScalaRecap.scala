package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, Scala") // Unit = "no meaningful value" = void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  val concat: (String, String) => String = _ + _
  val toUpperCase= (s:String) => s.toUpperCase()
  val yolo = toUpperCase(concat("a","b"))
  def concat2[B](a:B, b:B): String = String.valueOf(a)+String.valueOf(b)
  def concatFn = new Function2[String,String,String] {
    override def apply(v1: String, v2:String): String = v1+v2
  }
  println("Concat " + concat("foo","bar"))
  println("Concat " + concat2("foo","bar"))
  println("Concat " + concatFn("foo","bar"))
  println("Concat " + yolo)

  // map, flatMap, filter
  val processedList = List(1,2,3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _: Throwable => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.foreach(println)

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed: $ex")
  }

  // Partial functions
  val aPartialFunction: PartialFunction[Int, Int] = { // equivalent a un match sur x
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits

  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument
  println(implicitCall)

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet // fromStringToPerson("Bob").greet

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }
  "Lassie".bark

  /*
    // comment le cimpileur trouve le implicit qui va bien:
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
   */

}
