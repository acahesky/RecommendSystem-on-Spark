package base

import java.sql.Timestamp

import org.apache.spark.mllib.linalg.Vectors

case class UIData(userId:Int,itemId:Int,behaviorType:Int,userGeohash:String,itemCategory:Int,time:Timestamp) extends serializable
case class ItemData(item_id:Int,item_geohash:String,item_category:Int) extends serializable


case class UserData(userId:Int,userFeature:UserFeatures,uiSet: List[UISet]){
  def getUISet = {
    uiSet
  }
  def setUISet(newUISet:List[UISet]) = {
    UserData(userId,userFeature,newUISet)
  }
}

case class UserFeaturePerDate(visitSum:Int,collectSum:Int,shoppingcartSum:Int,purchaseSum:Int,
                              cv: Double,collectCV:Double,shoppingcartCV:Double,
                              visitDay:Int,collectDay:Int,shoppingcartDay:Int,purchaseDay:Int,
                              visitRate:Double,collectRate:Double,shoppingcartRate:Double,purchaseRate:Double,
                              activeDay:Int,activeRate:Double
                               ){
  def toArray={
    Array(visitSum,collectSum,shoppingcartSum,purchaseSum,
      cv,collectCV,shoppingcartCV,
      visitDay,collectDay,shoppingcartDay,purchaseDay,
      visitRate,collectRate,shoppingcartRate,purchaseRate,
      activeDay,activeRate)
  }
}
case class UserFeatures(firstLogin:Long,lastLogin:Long,daysLen:Double,
                        userFeatures:Array[UserFeaturePerDate]){
  def toArray = {
    Array(firstLogin.toDouble,lastLogin.toDouble,daysLen).union(userFeatures.flatMap(x => x.toArray))
  }
  def toDense = {
    Vectors.dense(
      this.toArray
        .map(x => if(x.isInfinity||x.isNaN||x.isInfinite) 0 else x)
    )
  }
}
object UserFeatures{
  val zeroVal = Array[UserFeaturePerDate]()
}

case class ItemFeaturePerDate(users:Int,purchaseSum:Int,purchaseUsers:Int,visitSum:Int,visitUsers:Int,collectSum:Int,collectUsers:Int,shoppingcartSum:Int,shoppingcartUsers:Int,cV:Double,userCV:Double,regularCustomerRate:Double,jumpOutRate:Double){
  def toArray = {
    Array(users,purchaseSum,purchaseUsers,visitSum,visitUsers,collectSum,collectUsers,shoppingcartSum,shoppingcartUsers,cV,userCV,regularCustomerRate,jumpOutRate)
    .map(x =>x.toDouble)
  }
}
case class ItemFeatures(itemFeatures: Array[ItemFeaturePerDate]){
  def toArray = {
    itemFeatures.flatMap(x => x.toArray)
  }
}
object ItemFeatures{
  val zeroVal = ItemFeatures(Array[ItemFeaturePerDate]())
}


case class UISet(ui:UI,userFeature:UserFeatures,itemFeature:ItemFeatures,uiFeature:UIFeatures){
  def getUI():UI ={
    ui
  }
//  def getUIList = {
//    uiList
//  }
  def setUIFeature(newUIFeature:UIFeatures)={
    UISet(ui,userFeature,itemFeature,newUIFeature)
  }
  def setItemFeature(newItemFeature:ItemFeatures)={
    UISet(ui,userFeature,newItemFeature,uiFeature)
  }
  def setUserFeature(newUserFeature:UserFeatures)={
    UISet(ui,newUserFeature,itemFeature,uiFeature)
  }
  def toDTFeature(label:Long) ={
    DTFeature(userFeature,itemFeature,uiFeature,label)
  }
}
case class UIFeaturePerDate(visitSum:Int,collectSum:Int,shoppingcartSum:Int,purchaseSum:Int,
                            cv: Double,collectCV:Double,shoppingcartCV:Double,
                            visitDay:Int,collectDay:Int,shoppingcartDay:Int,purchaseDay:Int,
                            visitRate:Double,collectRate:Double,shoppingcartRate:Double,purchaseRate:Double,
                            activeDay:Int,activeRate:Double){
  def toArray = {
    Array(visitSum,collectSum,shoppingcartSum,purchaseSum,
      cv,collectCV,shoppingcartCV,
      visitDay,collectDay,shoppingcartDay,purchaseDay,
      visitRate,collectRate,shoppingcartRate,purchaseRate,
      activeDay,activeRate)
  }
}
case class UIFeatures(rating:Double,firstAccess:Double,lastAccess:Double,daysLen:Double,uiFeatures : Array[UIFeaturePerDate])
{
  def toArray = {
    Array(rating,firstAccess,lastAccess,daysLen).union(uiFeatures.flatMap(x => x.toArray))
  }
}

case class UI(user:Int,item:Int)

case class DTFeature(u:UserFeatures, i:ItemFeatures,ui:UIFeatures, label:Long){
  def toDense ={
    Vectors.dense(
    u.toArray
    .union(i.toArray)
    .union(ui.toArray)
    .map(x => if(x.isInfinity||x.isNaN||x.isInfinite) 0 else x)
    )
  }
  def toUserFeaturesDense = {
    Vectors.dense(
      u.toArray
        .map(x => if(x.isInfinity||x.isNaN||x.isInfinite) 0 else x)
    )
  }

}

case class RatingWithDate(useId:Long,movieId:Long,rating:Double,date:Timestamp)

