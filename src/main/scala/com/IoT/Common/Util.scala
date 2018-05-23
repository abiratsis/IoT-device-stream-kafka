package com.IoT.Common

object Util {
  def greenPrint = (msg:String) => Console.out.println(Console.GREEN + msg +  Console.RESET)
  def redPrint = (msg:String) => Console.out.println(Console.RED + msg +  Console.RESET)

  private final val minDelayMS = 50
  private final val maxDelayMS = 250

  def randomSleep : Unit = {
    val rnd = new scala.util.Random
    Thread.sleep(minDelayMS + rnd.nextInt((maxDelayMS - minDelayMS) + 1))
  }
}
