package base

import java.sql.Timestamp

/**
 * Created by lenovo on 2015/4/14.
 */
case class UIData(userId:Int,itemId:Int,behaviorType:Int,userGeohash:String,itemCategory:Int,time:Timestamp) extends serializable
case class ItemData(item_id:Int,item_geohash:String,item_category:Int) extends serializable


case class UserData(userId:Int,userFeature:UserFeature,uiSet: List[UISet]){
  def getUISet = {
    uiSet
  }
  def setUISet(newUISet:List[UISet]) = {
    UserData(userId,userFeature,newUISet)
  }
}
case class UserFeature(c: Double,c1:Double,c2:Double,c3:Double,shoppingcartSum:Long)

case class UISet(ui:UI,uiFeature:UIFeature,uiList: List[UIData]){
  def getUI():UI ={
    ui
  }
  def setUIFeature(newUIFeature:UIFeature)={
    UISet(ui,newUIFeature,uiList)
  }
}
case class UIFeature(rating:Double)

case class UI(user:Int,Item:Int)