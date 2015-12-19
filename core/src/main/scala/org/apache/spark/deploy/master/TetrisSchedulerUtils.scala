package org.apache.spark.deploy.master

import scala.collection.mutable.{HashSet, ArrayBuffer}

object TetrisSchedulerUtils {

  //  Executor Utils
  val defaultCoresPerExecutor:Int = sys.env.getOrElse("tetrisDefaultCoresPerExecutor", "1").toInt

  /** Return whether the specified worker can launch an executor for this app. */
  def canLaunchExecutor(worker :WorkerInfo, app: ApplicationInfo): Boolean = {
    val coresPerExecutor:Int = app.desc.coresPerExecutor.getOrElse(defaultCoresPerExecutor)
    app.coresLeft >= coresPerExecutor && worker.coresFree >= coresPerExecutor && worker.memoryFree >= app.desc.memoryPerExecutorMB
  }

  /**
   * Calculates cosine similarity between a worker and a app if a executor can be launched on it
   * cos(0) = A dot B / |A| * |B|
   * @param worker Info about the worker
   * @param app Info about the app
   * @return Cosine similarity when a executor can be launched
   */
  def cosineSimilarity(worker :WorkerInfo, app: ApplicationInfo) :Option[Double] = {

    if(canLaunchExecutor(worker, app)) {
      def square(x:Double) =  x * x

      val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(defaultCoresPerExecutor)
      val num = worker.coresFree * coresPerExecutor + worker.memoryFree * app.desc.memoryPerExecutorMB
      val den = math.sqrt(square(worker.coresFree) + square(worker.memoryFree)) *
        math.sqrt(square(coresPerExecutor) + square(app.desc.memoryPerExecutorMB))

      Some(num / den)

    } else None
  }

  /**
   * Returns the Optimal executor,worker pair to launch given the current available resource resource
   * @param workers current worker resource availablity
   * @param waitingApps Apps to be scheduled
   * @return
   */
  def scheduleApplications
  (workers: HashSet[WorkerInfo],
   waitingApps: ArrayBuffer[ApplicationInfo]) : Option[(Double, WorkerInfo, ApplicationInfo)] = {

    var maxSimilarity: Option[(Double, WorkerInfo, ApplicationInfo)] = None
    for(app <- waitingApps if app.coresLeft > 0) {
      for(worker <- workers if worker.coresFree > 0) {
        val similarity = cosineSimilarity(worker, app)
        (similarity, maxSimilarity) match {
          case (Some(x:Double), None) => maxSimilarity = Some(x, worker, app)
          case (Some(x:Double), Some((max:Double, w:WorkerInfo, a:ApplicationInfo))) => if(x > max) maxSimilarity = Some(x, worker, app)
          case (_,_) =>
        }
      }
    }
    maxSimilarity
  }

//  Driver Utils
  def canLaunchDriver(worker :WorkerInfo, driver: DriverInfo): Boolean = {
    worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores
  }

  /**
   * Calculates cosine similarity between a worker and a driver
   * cos(0) = A dot B / |A| * |B|
   * @param worker Info about the worker
   * @param driver Info about the driver
   * @return Cosine similarity when a executor can be launched
   */
  def cosineSimilarity(worker :WorkerInfo, driver: DriverInfo) :Option[Double] = {

    if(canLaunchDriver(worker, driver)) {
      def square(x:Double) =  x * x

      val num = worker.coresFree * driver.desc.cores + worker.memoryFree * driver.desc.mem
      val den = math.sqrt(square(worker.coresFree) + square(worker.memoryFree)) *
        math.sqrt(square(worker.coresFree) + square(worker.memoryFree))

      Some(num / den)

    } else None
  }

  /**
   * Returns the Optimal driver, workerpair to launch given the current available resources
   * @param workers current worker resource availablity
   * @param waitingDrivers Drivers waiting to be scheduled
   * @return
   */
  def scheduleDrivers
  (workers: HashSet[WorkerInfo],
   waitingDrivers: ArrayBuffer[DriverInfo]) : Option[(Double, WorkerInfo, DriverInfo)] = {

    var maxSimilarity: Option[(Double, WorkerInfo, DriverInfo)] = None
    for(driver <- waitingDrivers if driver.desc.cores > 0) {
      for(worker <- workers if worker.coresFree > 0) {
        val similarity = cosineSimilarity(worker, driver)
        (similarity, maxSimilarity) match {
          case (Some(x:Double), None) => maxSimilarity = Some(x, worker, driver)
          case (Some(x:Double), Some((max:Double, w:WorkerInfo, d:DriverInfo))) => if(x > max) maxSimilarity = Some(x, worker, driver)
          case (_,_) =>
        }
      }
    }
    maxSimilarity
  }

}
