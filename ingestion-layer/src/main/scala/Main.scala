import requests._
import upickle.default._
object HelloWorld extends App {
  // get live matches
  val request = requests
    .get(
      url="https://footapi7.p.rapidapi.com/api/matches/live", 
        headers = Map(
          "X-RapidAPI-Key" -> "bfb32c3e32msh43fbdd4349980ffp1223e4jsned423ce098c3", 
          "X-RapidAPI-Host" -> "footapi7.p.rapidapi.com"
        )
    )

  println(ujson.read(request.text()))
}