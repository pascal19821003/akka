package cluster.transformation

object TransformationApp {

  def main(args: Array[String]): Unit = {
    TransformationFrontend.main(Seq("2551").toArray)
    TransformationBackend.main(Seq("2552").toArray)
    TransformationFrontend.main(Array.empty)
    TransformationBackend.main(Array.empty)
    TransformationBackend.main(Array.empty)
  }

}
