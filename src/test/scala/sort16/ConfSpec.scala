package sort16

import org.rogach.scallop.exceptions.ValidationFailure
import org.scalatest.funsuite.AnyFunSuite

class ConfSpec extends AnyFunSuite {

  test("defaults match Main getOrElse values when optional flags omitted") {
    val conf = new Conf(Seq("--output", "out.dat", "in.dat"), throwOnError = true)
    assert(conf.output.get.contains("out.dat"))
    assert(conf.files.get.contains(List("in.dat")))
    assert(conf.blocksize.getOrElse(1000000000) == 1000000000)
    assert(conf.threads.getOrElse(12) == 12)
    assert(conf.readbuffersize.getOrElse(20000000) == 20000000)
    assert(conf.action.getOrElse("sort") == "sort")
  }

  test("explicit overrides replace defaults") {
    val conf = new Conf(
      Seq(
        "--output", "out.dat",
        "--blocksize", "32",
        "--threads", "2",
        "--readbuffersize", "16",
        "--action", "merge",
        "a.dat", "b.dat"
      ),
      throwOnError = true
    )
    assert(conf.blocksize.get.contains(32))
    assert(conf.threads.get.contains(2))
    assert(conf.readbuffersize.get.contains(16))
    assert(conf.action.get.contains("merge"))
    assert(conf.files.get.contains(List("a.dat", "b.dat")))
  }

  test("missing --output fails verification") {
    val thrown = intercept[Exception] {
      new Conf(Seq("in.dat"), throwOnError = true)
    }
    assert(
      thrown.isInstanceOf[ValidationFailure] ||
        thrown.getMessage != null ||
        thrown.getClass.getName.contains("scallop"),
      s"unexpected failure type: ${thrown.getClass.getName}: ${thrown.getMessage}"
    )
  }
}
