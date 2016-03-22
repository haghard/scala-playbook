package raptire_examples

import org.specs2.mutable.Specification

import scala.util.{ Success, Failure }

class RaptureSpec extends Specification {

  "Parse json" should {
    import rapture.json.jsonBackends.spray._

    "try as effect" in {
      import rapture.core.modes.returnTry._
      val json = rapture.json.Json.parse(" [ 1, 2, 3 ")

      val r = json match {
        case Success(r)  ⇒ 0
        case Failure(ex) ⇒ 1
      }

      r === 1
    }

    "option as effect" in {
      import rapture.core.modes.returnOption._
      val json = rapture.json.Json.parse(" [ 1, 2, 3 ")
      json === None
    }
    /*
    "future as effect" in {
      import rapture.core.modes.returnResult._
      val json = rapture.json.Json.parse(" [ 1, 2, 3 ")
      json === None
    }*/
  }

  "internationalized strings" should {
    "create strings" in {

      import rapture.i18n._
      val greeting: I18n[String, En with Fr with De] = en"Hello" | fr"Bonjour" | de"Guten Tag"
      val msgs: I18n[String, Fr with En with De] = fr"Bonjour" | en"Hello" | de"Guten Tag"
      val msgs1: I18n[String, En with De] = en"Hello" | de"Guten Tag"

      val greeting1 = en"Hello" | fr"Bonjour"

      en"In England we greet people with '$greeting1'."

      //compiler error
      //de"In England we greet people with '$greeting1'."

      1 === 1
    }

    "use default languages" in {
      import rapture.i18n._

      object Messages extends LanguageBundle[En with Fr with Ru] {
        val error: IString = en"Hello world" | fr"Bonjour le monde" | ru"Привет мир!"
      }

      import languages.ru._
      val str: String = Messages.error
      str === "Привет мир!"
    }
  }
}