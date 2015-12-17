package org.apache.spark.deploy.master

import scala.collection.mutable
import scala.collection.mutable.{HashSet, ArrayBuffer}

object TetrisSchedulerUtils {

  val defaultCoresPerExecutor = 1

  /** Return whether the specified worker can launch an executor for this app. */
  def canLaunchExecutor(worker :WorkerInfo, app: ApplicationInfo): Boolean = {
    val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(defaultCoresPerExecutor)
    app.coresLeft >= coresPerExecutor && worker.coresFree >= coresPerExecutor && worker.memoryFree >= app.desc.memoryPerExecutorMB
  }

  /**
   * Calculates cosine similarity between a worker and a app if a executor can be launched on it
   * cos(0) = A dot B / |A| * |B|
   * @param worker
   * @param app
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
   *
   * @param workers
   * @param waitingApps
   * @return
   */
  def scheduleApplications
  (workers: HashSet[WorkerInfo],
   waitingApps: ArrayBuffer[ApplicationInfo]) : Option[(Double, WorkerInfo, ApplicationInfo)] = {

    var maxSimilarity: Option[(Double, WorkerInfo, ApplicationInfo)] = None
    for(app <- waitingApps if app.coresLeft > 0) {
      for(worker <- workers if worker.coresFree > 0) {
        if (canLaunchExecutor(worker, app)) {
          val similarity = cosineSimilarity(worker, app)
          (similarity, maxSimilarity) match {
            case (Some(x:Double), None) => maxSimilarity = Some(x, worker, app)
            case (Some(x:Double), Some((max:Double, w:WorkerInfo, a:ApplicationInfo))) => if(x > max) maxSimilarity = Some(x, worker, app)
            case (_,_) =>
          }
        }
      }
    }
    maxSimilarity
  }






}
