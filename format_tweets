def requestFormatter(givenTweet:String):String={
  s"""{
    "documents":[
        {
        "language":"en",
        "id":1,
        "text":"${givenTweet}"
        }
    ]
  }"""
}

def sendPostRequest(textAnalyticsUrl:String,subscriptionKey:String,requestBody:String):String={
  import scalaj.http.Http
  Thread.sleep(3000)
  val result = Http(textAnalyticsUrl).postData(requestBody)
  .header("Content-Type", "application/json")
  .header("Ocp-Apim-Subscription-Key", subscriptionKey).asString
  result.body
}

def removeHttpLines(textLine:String):Boolean={
  import scala.util.matching.Regex
  val pattern = "^http".r
  pattern.findFirstIn(textLine) match {
    case Some(x)=>false
    case _ => true
  }
}

val tweetsSentimentsRdd = sc.textFile("t_tweets.txt").filter(removeHttpLines).map(x=>requestFormatter(x)).map(y=>sendPostRequest("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.1/sentiment","27ccdce0dd5c44949a28c70b38a28452",y))

val tweetsSentimentList = tweetsSentimentsRdd.collect()
println(tweetsSentimentList)

println(tweetsSentimentList.length)

case class ResponseBody(id:String, score:Double)
case class AzureTextAnalyticsResponse(documents: List[ResponseBody], errors: List[String])

object ResponseJsonUtility extends java.io.Serializable {
 import spray.json._
 import DefaultJsonProtocol._
object MyJsonProtocol extends DefaultJsonProtocol {
 implicit val responseBodyFormat = jsonFormat(ResponseBody,"id","score") //this represents the inner document object of the Json
 implicit val responseFormat = jsonFormat(AzureTextAnalyticsResponse,"documents","errors") //this represents the outer key-value pairs of the Json
 }
//and lastly, a function to parse the Json (string) needs to be written which after parsing the Json string returns data in the form of case class object.
import MyJsonProtocol._
 import spray.json._
 
 def parser(givenJson:String):AzureTextAnalyticsResponse = {
 givenJson.parseJson.convertTo[AzureTextAnalyticsResponse]
 }
}

val tweetsSentimentScore = tweetsSentimentList.filter(eachResponse=>eachResponse.contains("documents")).map(eachResponse=>ResponseJsonUtility.parser(eachResponse)).map(parsedResponse=>parsedResponse.documents(0).score)

//Average
(tweetsSentimentScore.sum)/(tweetsSentimentScore.length)
//println(tweetsSentimentScore.length)

tweetsSentimentScore.max

tweetsSentimentScore.min

//to count the number of positive sentiments
var pcnt = 0
for(i<-tweetsSentimentScore){
    if(i>0.5){
      pcnt +=1
      print(i)
    }
      
} 
println("")
println("Count of Positive Tweets: ",pcnt)

//to count the number of neutral sentiments
var pcnt = 0
for(i<-tweetsSentimentScore){
    if(i==0.5){
      pcnt +=1
      print(i)
    }
      
} 
println("")
println("Count of Neutral Tweets: ",pcnt)

//to count the number of negative sentiments
var ncnt = 0
for(i<-tweetsSentimentScore){
    if(i<0.5){
       ncnt +=1
      print(i)
    }
     
} 
println("")
println("Count of Negative Tweets: ",ncnt)
