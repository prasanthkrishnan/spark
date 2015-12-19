package org.apache.spark.deploy.master

import java.util.Date

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.{SecurityManager, SparkConf}
import org.scalatest.FunSuite

import scala.collection.mutable

class TetrisSchedulerUtils$Test extends FunSuite {

    private val workerInfo = makeWorkerInfo(4096, 10)
    private val workerInfos = mutable.HashSet(workerInfo, workerInfo, workerInfo)

    private def makeMaster(conf: SparkConf = new SparkConf): Master = {
      val securityMgr = new SecurityManager(conf)
      val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityMgr)
      val master = new Master(rpcEnv, rpcEnv.address, 0, securityMgr, conf)
      master
    }

    private def makeAppInfo( appId: String = System.currentTimeMillis.toString,
                             memoryPerExecutorMb: Int,
                             coresPerExecutor: Option[Int] = None,
                             maxCores: Option[Int] = None): ApplicationInfo = {
      val desc = new ApplicationDescription(
        "test", maxCores, memoryPerExecutorMb, null, "", None, None, coresPerExecutor)

      new ApplicationInfo(0, appId, desc, new Date, null, Int.MaxValue)
    }

    private def makeWorkerInfo(memoryMb: Int, cores: Int): WorkerInfo = {
      val workerId = System.currentTimeMillis.toString
      new WorkerInfo(workerId, "host", 100, cores, memoryMb, null, 101, "address")
    }

  test("testCosineSimilarity between worker and driver") {

//    TetrisSchedulerUtils.cosineSimilarity(workerInfo,)
  }

  test("testScheduleDrivers") {

  }

  test("testCosineSimilarity With worker and Executor Should return None if unable to schedule - Insufficient Memory") {
    val appInfo = makeAppInfo("1", 4096 * 4096,coresPerExecutor=Option(1))
    val similarity = TetrisSchedulerUtils.cosineSimilarity(workerInfo, appInfo)
    assert(similarity.isEmpty)
  }

  test("testCosineSimilarity With worker and Executor Should return None if unable to schedule - Insufficient Cores") {
    val appInfo = makeAppInfo("1", 4096,coresPerExecutor=Option(100))
    val similarity = TetrisSchedulerUtils.cosineSimilarity(workerInfo, appInfo)
    assert(similarity.isEmpty)
  }

  test("testCosineSimilarity With worker and Executor for perfect similarity") {
    val appInfo = makeAppInfo("1", 4096,coresPerExecutor=Option(10))
    val similarity = TetrisSchedulerUtils.cosineSimilarity(workerInfo, appInfo)
    assert(similarity.isDefined)
    assert(1.0 == similarity.get)
  }


  test("testScheduleApplications must schedule highest matching executor Exact Match") {
    val appInfo1 = makeAppInfo("1", 4096, Option(10), Option(30))
    val appInfo2 = makeAppInfo("2", 2096, Option(5), Option(15))
    val appInfo3 = makeAppInfo("3", 8096, Option(15), Option(45))
    val appInfo = mutable.ArrayBuffer(appInfo1,appInfo2,appInfo3)
    val scheduledApp = TetrisSchedulerUtils.scheduleApplications(workerInfos, appInfo)
    assert(scheduledApp.isDefined)
    assert(scheduledApp.get._1 == 1)
    assert(scheduledApp.get._3.id == "1")
  }

  test("testScheduleApplications must schedule highest matching executor") {
    val appInfo1 = makeAppInfo("1", 3966, Option(8), Option(24))
    val appInfo2 = makeAppInfo("2", 2096, Option(5), Option(15))
    val appInfo3 = makeAppInfo("3", 8096, Option(15), Option(45))
    val appInfo = mutable.ArrayBuffer(appInfo1,appInfo2,appInfo3)
    val scheduledApp = TetrisSchedulerUtils.scheduleApplications(workerInfos, appInfo)
    assert(scheduledApp.isDefined)
    assert(scheduledApp.get._3.id == "2")
  }

}
