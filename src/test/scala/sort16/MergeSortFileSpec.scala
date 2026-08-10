package sort16

import org.scalatest.funsuite.AnyFunSuite
import sort16.support.RecordIo._

class MergeSortFileSpec extends AnyFunSuite {

  test("merges multiple sorted runs into global order") {
    withTempDir { dir =>
      val run1 = dir.resolve("r1.dat")
      val run2 = dir.resolve("r2.dat")
      writeDat(run1, Seq((1, 0, 0, 0), (4, 0, 0, 0), (7, 0, 0, 0)))
      writeDat(run2, Seq((2, 0, 0, 0), (3, 0, 0, 0), (9, 0, 0, 0)))
      val output = dir.resolve("out.dat")

      val merge = new MergeSort(Vector(run1.toString, run2.toString), output.toString, readBufferSize = 48)
      merge.init()
      merge.sort()

      val sorted = readDat(output)
      assert(sorted.size == 6)
      assert(isNonDecreasing(sorted))
      assert(sorted == Seq(
        (1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0),
        (4, 0, 0, 0), (7, 0, 0, 0), (9, 0, 0, 0)
      ))
    }
  }

  test("tiny readbuffersize forces refill and still merges correctly") {
    withTempDir { dir =>
      // 4 records in one run; buffer of 16 bytes => one record per refill
      val run = dir.resolve("r.dat")
      writeDat(run, Seq((1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (4, 0, 0, 0)))
      val output = dir.resolve("out.dat")

      val merge = new MergeSort(Vector(run.toString), output.toString, readBufferSize = 16)
      merge.init()
      merge.sort()

      assert(readDat(output) == Seq((1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (4, 0, 0, 0)))
    }
  }

  test("merge-only path sorts pre-sorted runs without batch partition") {
    withTempDir { dir =>
      val a = dir.resolve("a.dat")
      val b = dir.resolve("b.dat")
      writeDat(a, Seq((1, 0, 0, 0), (5, 0, 0, 0)))
      writeDat(b, Seq((2, 0, 0, 0), (3, 0, 0, 0)))
      val output = dir.resolve("out.dat")

      // Same path as non-sort action: MergeSort only
      val merge = new MergeSort(Vector(a.toString, b.toString), output.toString, readBufferSize = 16)
      merge.init()
      merge.sort()

      val names = dir.toFile.list().toSet
      assert(!names.exists(n => n.matches(""".*\.\d+""") && !n.startsWith("out")))

      assert(readDat(output) == Seq((1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (5, 0, 0, 0)))
    }
  }
}
