import akka.actor.{Actor,ActorRef,ActorSystem,Props}
import scala.collection.mutable.ArrayBuffer
import scala.math.{abs,pow,cbrt}
import akka.actor.PoisonPill
import scala.util.Random
    
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import akka.actor.Cancellable;
import scala.concurrent.duration._



object GossipSimulator{
  
  case class gossipStringFromSensor(topology:String,algorithm:String,numberOfNodes:Integer,sensorArrayBuffer:ArrayBuffer[ActorRef])
    
    /*var out_file = new java.io.FileOutputStream("output.txt")
    var out_stream = new java.io.PrintStream(out_file)
    */
  val startTime = System.currentTimeMillis;
  

  
   def main(arg:Array[String]){
    
    
  

  val system = ActorSystem("mySystem")
  
  var numberOfSensors:Integer=arg(0).toInt
  var inputTopology:String=arg(1)
  var inputAlgorithm:String=arg(2)
  var inputSumAvg:String=arg(3)
  
  inputTopology=inputTopology.toLowerCase()
  inputAlgorithm=inputAlgorithm.toLowerCase()
  inputSumAvg=inputSumAvg.toLowerCase()
  
  /*numberOfSensors=1000
  inputTopology="full"
  inputAlgorithm="pushsum"
  inputSumAvg="avg"*/
  
  

  
  

  
  var sensorArray:ArrayBuffer[ActorRef]=new ArrayBuffer[ActorRef](numberOfSensors)
  
  if(inputSumAvg.equals("avg"))
    for(i<-0 to numberOfSensors-1){
      val newSensor = system.actorOf(Props(new Sensor(i,1)), "sensor"+i)
      sensorArray+=newSensor
  }
  else if(inputSumAvg.equals("sum")){
      for(i<-0 to numberOfSensors-1){
      val newSensor = system.actorOf(Props(new Sensor(i,0)), "sensor"+i)
      sensorArray+=newSensor
    }
  }
  
  if(inputAlgorithm.equals("pushsum"))
  {
     sensorArray(0)!inputFromSensor(0,1,inputTopology,inputAlgorithm,numberOfSensors,sensorArray)

  }
  else if(inputAlgorithm.equals("gossip")){
   sensorArray(0)!gossipStringFromSensor(inputTopology,inputAlgorithm,numberOfSensors,sensorArray)

  }
  
  //system.scheduler.schedule(Duration.Zero, Duration.create(100, TimeUnit.MILLISECONDS), sensorArray(0), "gg")
 
 //sensorArray(0)!gossipStringFromSensor("3D","gossip",numberOfSensors,sensorArray)
 


  
  
 
 
 sensorArray(0)!inputFromSensor(0,1,"full","pushSum",numberOfSensors,sensorArray)
  
 
 
  
    }
  









//-------------------------------------------------------------------------------------------------------------------------------------------------------//



case class inputFromSensor(value:Float,weight:Float,topology:String,algorithm:String,numberOfNodes:Integer,sensorArrayBuffer:ArrayBuffer[ActorRef])


class Sensor(value:Float,weight:Float) extends Actor{
  var stateValue:Float=value
  var stateWeight:Float=weight
  var statePropagationCount=0
  var gossipCount=0
  
  def receive={
        case inputFromSensor(inputValue,inputWeight,topology,algorithm,numNodes,sensorArrayBuffer)=>
            
            var oldRatio=stateValue/stateWeight
            var newRatio=(stateValue+inputValue)/(stateWeight+inputWeight)
            
            
            printThis("Message at Actor "+sensorArrayBuffer.indexOf(self))
            printThis("Old S= "+stateValue+" Old Weight= "+ stateWeight+" OldRatio= "+oldRatio)
            printThis("New S= "+stateValue+inputValue+" New Weight= "+ stateWeight+inputWeight+" NewRatio= "+newRatio)
            printThis("Difference in Ratios= "+abs(oldRatio-newRatio))
            
            
           if(abs(oldRatio-newRatio)<pow(10,-10)){
             statePropagationCount+=1
           }
           else{
             statePropagationCount=0
           }
            if(statePropagationCount>3){
              printThis("Actor "+self.toString()+" terminated")
              context.stop(self)
              printThis("Seconds terminated for topology to terminate "+((System.currentTimeMillis-startTime)/1000))
              //out_stream.close
              
              //self ! PoisonPill
            }
            else{
            stateValue=(stateValue+inputValue)/2
            stateWeight=(stateWeight+inputWeight)/2
            
            
            
            var neighbours:ArrayBuffer[Integer]=getNeighbours(topology,numNodes,sensorArrayBuffer)
            printThis("Topology = "+topology)
            printThis("Neighbours length= "+neighbours.length)
            /*for(i<-neighbours){
              printThis("neighbours= "+i.toString())
            }*/
            
            var randomNeighbourIndex=Random.nextInt(neighbours.length)
            printThis("Node Selected by random= "+neighbours(randomNeighbourIndex))
            printThis("\n")
            
            
            
            sensorArrayBuffer(neighbours(randomNeighbourIndex)) ! inputFromSensor(stateValue,stateWeight,topology,"pushSum",numNodes,sensorArrayBuffer)
              
            }
            
            
        case gossipStringFromSensor(topology,algorithm,numNodes,sensorArrayBuffer)=>
          printThis("Received"+ gossipCount +"  message to gossip sensor at sensor"+sensorArrayBuffer.indexOf(self))
          
         
          
          var neighbours:ArrayBuffer[Integer]=getNeighbours(topology,numNodes,sensorArrayBuffer)
          var randomNeighbourIndex=Random.nextInt(neighbours.length)
          
          if(gossipCount<10)
          {
            gossipCount+=1
            
            
            sensorArrayBuffer(neighbours(randomNeighbourIndex))!gossipStringFromSensor(topology,algorithm,numNodes,sensorArrayBuffer)
                
          }
          else{
            //context.stop(self)
            sensorArrayBuffer(neighbours(randomNeighbourIndex))!gossipStringFromSensor(topology,algorithm,numNodes,sensorArrayBuffer)

            self! PoisonPill
            printThis("Actor "+sensorArrayBuffer.indexOf(self)+" terminated")
          }
        
        }
        
  
  def getNeighbours(toplogy:String,numNodes:Integer,sensorArrayBuffer:ArrayBuffer[ActorRef]):ArrayBuffer[Integer]=
  {
    var latticeCount=cbrt(numNodes.toDouble)
    var latticeCt=latticeCount.asInstanceOf[Int]
    var neighbours:ArrayBuffer[Integer]=ArrayBuffer[Integer]()
    val selfIndex=sensorArrayBuffer.indexOf(self)
    if(toplogy.equals("line")){
      neighbours.clear()
      
      
      
      if(sensorArrayBuffer.isDefinedAt(selfIndex-1)){
        neighbours+=(selfIndex-1)
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+1)){
        neighbours+=(selfIndex+1)
      }
      /*for(n<-neighbours){
      printThis("Neighbour sensors = "+(n).toString())
      }*/
      return neighbours
    }
    else if(toplogy.equals("full")){
      neighbours.clear()
      for(i<-0 to sensorArrayBuffer.length-1){
        neighbours+=i
      }
      neighbours=neighbours-sensorArrayBuffer.indexOf(self)
      return neighbours
    }
    else if(toplogy.equals("3d")){
      neighbours.clear()
      
      if(sensorArrayBuffer.isDefinedAt(selfIndex-(latticeCt*latticeCt))){
        neighbours=neighbours+=(selfIndex-(latticeCt*latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+(latticeCt*latticeCt))){
        neighbours=neighbours+=(selfIndex+(latticeCt*latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex-(latticeCt))){
        neighbours=neighbours+=(selfIndex-(latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+(latticeCt))){
        neighbours=neighbours+=(selfIndex+(latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex-1)){
        neighbours=neighbours+=(selfIndex-1)
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+1)){
        neighbours=neighbours+=(selfIndex+1)
      }
      return neighbours
     }
    else if(toplogy.equals("imperfect3D")){
      neighbours.clear()
      
      if(sensorArrayBuffer.isDefinedAt(selfIndex-(latticeCt*latticeCt))){
        neighbours=neighbours+=(selfIndex-(latticeCt*latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+(latticeCt*latticeCt))){
        neighbours=neighbours+=(selfIndex+(latticeCt*latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex-(latticeCt))){
        neighbours=neighbours+=(selfIndex-(latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+(latticeCt))){
        neighbours=neighbours+=(selfIndex+(latticeCt))
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex-1)){
        neighbours=neighbours+=(selfIndex-1)
      }
      if(sensorArrayBuffer.isDefinedAt(selfIndex+1)){
        neighbours=neighbours+=(selfIndex+1)
      }
      
      var allExceptNeighbours:ArrayBuffer[Integer]=ArrayBuffer[Integer]()
      for(i<-0 to sensorArrayBuffer.length-1){
        allExceptNeighbours+=i
      }
      
      for(neigh<-neighbours){
        allExceptNeighbours=allExceptNeighbours-neigh
      }
      var randomNeighbourFromExceptNb=allExceptNeighbours(Random.nextInt(allExceptNeighbours.length))
      
      neighbours+=randomNeighbourFromExceptNb

      
      return neighbours
     }
    else{
      return neighbours
    }
    }
  
  def printThis(str:String){
  
    println(str)
    /*out_stream.print(str)
    out_stream.print("\n")*/
    
  }
   
 }
}