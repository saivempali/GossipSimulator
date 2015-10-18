import akka.actor.Actor
import akka.actor._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorSystem
import akka.actor.{ActorRef,Props}
import akka.routing.RoundRobinPool
import scala.math


object project_2 {
  def main(args:Array[String]){
    val msg = "let's gossip"
    var system = ActorSystem("Gossip")
    val numNodes = args(0).toInt
    var nodes = new ArrayBuffer[ActorRef](numNodes)
    var S = Array.ofDim[Float](numNodes)
    // initializing s array
    for (i <- 0 to numNodes-1){
      S(i)= i
    }
    // initializing w array
    var W = Array.ofDim[Float](numNodes)
    for (i <- 0 to numNodes-1){
      W(i)= 1
    }
    var track = Array.ofDim[Int](numNodes)
    for (i <- 0 to numNodes-1){
      track(i)= 0
    }
    
    // creating nodes
    var i = 0
      while (i!=numNodes){
        nodes += system.actorOf(Props[node],name = "node"+i.toString())
        i+=1
      }
    //Starting the rumour - args(1)=topology, args(2)=algorithm
    nodes(scala.util.Random.nextInt(numNodes)) ! startRumour(numNodes,args(1),args(2),nodes,S,W,msg,track)
  }
}

case class startRumour(numNodes:Int, topology:String, algo:String,nodeArray:ArrayBuffer[ActorRef],S:Array[Float],W:Array[Float],msg:String,track:Array[Int])
case class gossip(msg:String,topology:String,nodeArray:ArrayBuffer[ActorRef],track:Array[Int])
case class pushSum(topology:String,nodeArray:ArrayBuffer[ActorRef],s:Float,w:Float,S:Array[Float],W:Array[Float],track:Array[Int])

class node extends Actor{
  var numNodes = 0
  var topology =""
  var algo =""
  var Gossip = ""
  var GossipCount = 0
  var PushSumCount = 0
  var newRatio:Float = 0
  var oldRatio:Float = 0
  def receive={
        case gossip(msg,topology,nodeArray,track) =>
          GossipCount += 1
          if(GossipCount <= 10){
            val nextNode = neighbours.getNextIndex(nodeArray, topology, numNodes,track)
            println("Received msg by node - "+nodeArray.indexOf(self))
            println("")
            nodeArray(nextNode) ! gossip(msg,topology,nodeArray,track)           
          }
          else{
            val nextNode = neighbours.getNextIndex(nodeArray, topology, numNodes,track)
            println("Received msg by node - "+nodeArray.indexOf(self))
            println("")
            nodeArray(nextNode) ! gossip(msg,topology,nodeArray,track)      
            println("I'm terminating"+nodeArray.indexOf(self))
            //self ! PoisonPill
            track(nodeArray.indexOf(self)) = 1
            
            }
            
    //a & b are the s & w values that were updated and sent to the next node 
    case pushSum(topology,nodeArray,a,b,s,w,track) =>
        val nextNode = neighbours.getNextIndex(nodeArray, topology, nodeArray.size,track)
        println("Received msg by node - "+nodeArray.indexOf(self))
        val index = nodeArray.indexOf(self)
        s(index) += a
        w(index) += b
        s(index) = s(index)/2
        w(index) = w(index)/2
        newRatio = s(index)/w(index)
        println("New Ratio = " + newRatio + "     Old Ratio = " + oldRatio)
        println("-----------------------------------------------------")
        if(math.abs(newRatio - oldRatio) < math.pow(10,-10)){
            PushSumCount += 1
        }
        else{
          PushSumCount = 0
        }
        if(PushSumCount == 3){
          println("terminated  ")
          self ! PoisonPill
        }
        else{
        oldRatio = newRatio  
        nodeArray(nextNode) ! pushSum(topology,nodeArray,s(index),w(index),s,w,track)
        }
    case startRumour(n,topo,alg,nodeArray,s,w,msg,track) =>
      numNodes = n 
      topology = topo
      algo = alg
      Gossip = msg
      algo.toLowerCase() match
      {
        // gossip case
        case "gossip"=>
          GossipCount += 1
          if(GossipCount <= 1000){
            val nextNode = neighbours.getNextIndex(nodeArray, topology, numNodes,track)
            println("Starting the Rumour by node " + nodeArray.indexOf(self))
            println("-----------------------------------------------------")
            nodeArray(nextNode) ! gossip(msg,topology,nodeArray,track)              
          }
          else{
            println("terminating "+nodeArray.indexOf(self))
            context.stop(self)
          }
        // Push Sum case    
        case "pushsum" =>
          for (i <- 0 to numNodes-1){
            track(i)= 0
          }
          println("Starting the Rumour by node " + nodeArray.indexOf(self))
          println("-----------------------------------------------------")
          val nextNode = neighbours.getNextIndex(nodeArray, topology, numNodes,track)
          val index = nodeArray.indexOf(self)
          s(index) = s(index)/2
          w(index) = w(index)/2
          oldRatio = s(index)/w(index)
          nodeArray(nextNode) ! pushSum(topology,nodeArray,s(index),w(index),s,w,track)
      }    
  }

object neighbours{
def getNextIndex(nodeArray:ArrayBuffer[ActorRef],topology:String,numNodes:Int,track:Array[Int]): Int = {
  topology.toLowerCase() match{
            case "full" =>
              val b = new ListBuffer[Int]()
              for(i <- 0 to nodeArray.length-1){
                b += i
              }
              b -= nodeArray.indexOf(self)
              val nextIndex = scala.util.Random.nextInt(b.size)
              if(track(nodeArray.indexOf(self)) == 1){
                          
              }
                
              return b(nextIndex)

            case "3d" =>
              val currentIndex = nodeArray.indexOf(self)
              val x = (math.ceil(math.cbrt(numNodes))).toInt
              val xSquare = (math.pow(x,2)).toInt
              val a = new ListBuffer[Int]
              println("current = "+currentIndex)
              if(nodeArray.isDefinedAt(currentIndex - 1)){
                a += currentIndex-1
              }
              if(nodeArray.isDefinedAt(currentIndex + 1)){
                a +=(currentIndex + 1)
              }
              if(nodeArray.isDefinedAt(currentIndex - x)){
                a +=(currentIndex - x)
              }
              if(nodeArray.isDefinedAt(currentIndex + x)){
                a +=(currentIndex + x)
              }
              if(nodeArray.isDefinedAt(currentIndex - xSquare)){
                a +=(currentIndex - xSquare)
              }
              if(nodeArray.isDefinedAt(currentIndex + xSquare)){
                a +=(currentIndex + xSquare)
              }
              var nextIndex = scala.util.Random.nextInt(a.length)
              return a(nextIndex)
              
            case "line" =>
              val currentIndex = nodeArray.indexOf(self)
              val a = new ListBuffer[Int]()
              if(nodeArray.isDefinedAt(currentIndex - 1)){
                a += currentIndex-1
              }
              if(nodeArray.isDefinedAt(currentIndex + 1)){
                a +=(currentIndex + 1)
              }
              println("neighbors = "+a)
              val nextIndex = scala.util.Random.nextInt(a.size)
              return a(nextIndex)
              
            case "imperfect3d" =>
              val currentIndex = nodeArray.indexOf(self)
              val x = (math.floor(math.pow(numNodes,0.34))).toInt
              val xSquare = (math.pow(x,2)).toInt
              val a = new ListBuffer[Int]()
              if(nodeArray.isDefinedAt(currentIndex - 1)){
                a += currentIndex-1
              }
              if(nodeArray.isDefinedAt(currentIndex + 1)){
                a +=(currentIndex + 1)
              }
              if(nodeArray.isDefinedAt(currentIndex - x)){
                a +=(currentIndex - x)
              }
              if(nodeArray.isDefinedAt(currentIndex + x)){
                a +=(currentIndex + x)
              }
              if(nodeArray.isDefinedAt(currentIndex - xSquare)){
                a +=(currentIndex - xSquare)
              }
              if(nodeArray.isDefinedAt(currentIndex + xSquare)){
                a +=(currentIndex + xSquare)
              }
              val b = new ListBuffer[Int]()
              for(i <- 0 to nodeArray.length-1){
                b += i
              }
              for (i <- 0 until a.length-1){
                b -= a(i)
              }
              val extraIndex = scala.util.Random.nextInt(b.size)
              a += extraIndex
              val nextIndex = scala.util.Random.nextInt(a.size)
              return a(nextIndex)
          }
 }
}
}