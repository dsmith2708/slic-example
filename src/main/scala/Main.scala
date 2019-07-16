import slick.jdbc.MySQLProfile.api._

import scala.collection.immutable.Range
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object
Main extends App {

  // The config string refers to mysqlDB that we defined in application.conf
  val db = Database.forConfig("mysqlDB")
  // represents the actual table on which we will be building queries on
  val peopleTable = TableQuery[People]

  val dropPeopleCmd = DBIO.seq(peopleTable.schema.drop)
  val initPeopleCmd = DBIO.seq(peopleTable.schema.create)

  def dropPeopleTable(): Future[Unit] = {
    val dropFuture = Future{
      db.run(dropPeopleCmd)
    }

    Await.result(dropFuture, Duration.Inf).andThen {
      case Success(_) => println("People table dropped"); createAndPopulatePeopleTable()
      case Failure(error) => println("problem: " + error.getMessage); createAndPopulatePeopleTable()
    }
  }

  def createAndPopulatePeopleTable(): Unit = {
    val setupTable = Future{
      db.run(initPeopleCmd)
    }
    Await.result(setupTable, Duration.Inf).andThen {
      case Success (_) => {
        val addRecords = Future {
          val querytoRun = peopleTable ++= Seq(
            (10, "Jack", "Wood", 30),
            (11, "Jonathon", "Warren", 31),
            (12, "Jim", "Walker", 32),
            (13, "John", "Wick", 33),
            (14, "Jon", "Watson", 34),
            (15, "James", "Watts", 35),
            (16, "Janet", "Wales", 36),
            (17, "Jane", "Watt", 37),
            (18, "Janey", "Wong", 38),
            (19, "Juliet", "Waters", 39),
            (20, "Jack", "Webber", 40),
            (21, "Jack", "Waters", 41),
            (22, "Jack", "Watson", 42),
            (23, "J", "W", 40),
            (24, "Ji", "Wa", 41),
            (25, "Joooooooob", "Waaaaaaaaaat", 42),


          )
          db.run(querytoRun)
        }
        Await.result(addRecords, Duration.Inf).andThen {
          case Success (_) => println("People Added"); getAllPeople()
          case Failure (error) => println("Error: " + error.getMessage)
        }
      }
      case Failure (error) => println("Error: " + error.getMessage)
    }
  }

  def getAllPeople(): Future[Unit] = {
    val getAllQuery = Future {
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
    }
    Await.result(getAllQuery, Duration.Inf).andThen {
      case Success (_) => println("Records Printed"); printAllBeginningWithJoDeleteAndPrintResult()
      case Failure (error) => println("Error: " + error.getMessage)
    }

  }

  def printAllBeginningWithJoDeleteAndPrintResult() = {
    val getJoNamesQuery = Future {
      val allResults = db.run(peopleTable.result)
      allResults.map(_.filter {
        case (id, fName, lName, age) => fName.startsWith("Jo")
      }.foreach{
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
      val deleteAction = peopleTable.filter( p => p.fName.startsWith("Jo")).delete
      db.run(deleteAction)
    }

    Await.result(getJoNamesQuery, Duration.Inf).andThen {
      case Success (_) => println("get and delete Jo Names succeeded"); printResult()
      case Failure (error) => println("get Jo Names failed: " + error.getMessage)
    }
  }

  def printResult(): Unit = {
    val getAllQuery = Future {
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
    }
    Await.result(getAllQuery, Duration.Inf).andThen {
      case Success (_) => println("Records Printed"); getAllWithSameFirstName()
      case Failure (error) => println("Error: " + error.getMessage)
    }
  }

  def getAllWithSameFirstName() = {
    val firstNameToMatch = "Jack"
    println("Getting all with same first name")
    val getAllWithSameFirstNameQuery = Future {
      val filter = peopleTable.filter {
        p => p.fName === firstNameToMatch
      }
      val action = filter.result
      val actionResult = db.run(action)
      actionResult.map {
        case x => println(s"found ${x.length} results")
      }
      actionResult.map(_.foreach {
        case (id, fName, lName, age) => println(s"$id $fName $lName $age")
      })
    }

    Await.result(getAllWithSameFirstNameQuery, Duration.Inf).andThen {
      case Success (_) => println("got all with same first name"); getAllFourToSixCharactersLong()
      case Failure (error) => println("Error getting all with same first name: " + error.getMessage)
    }
  }

  def getAllFourToSixCharactersLong(): Unit = {
    println("getting all first names 4-6 characters long")
    val getNamesFourToSixFuture = Future{
      val filterToUse = peopleTable.filter {
        p => p.fName.length <= 6 && p.fName.length >= 4
      }
      val result = db.run(filterToUse.result)

      result.map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
    }

    Await.result(getNamesFourToSixFuture, Duration.Inf).andThen {
      case Success (_) => println("got all names 4-6 long")
      case Failure (error) => println("error getting names 4-6 characters long " + error.getMessage)
    }
  }



  dropPeopleTable()
  Thread.sleep(5000)


}