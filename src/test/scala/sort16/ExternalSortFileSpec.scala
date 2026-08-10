package sort16

import org.scalatest.funsuite.AnyFunSuite
import sort16.support.RecordIo._

import java.nio.file.{Files, Path}

class ExternalSortFileSpec extends AnyFunSuite {

  private def endToEndSort(
      dir: Path,
      inputs: Seq[(String, Seq[Record])],
      blockSize: Int,
      threads: Int = 1,
      readBufferSize: Int = 16
  ): (Path, Vector[String], Seq[Record]) = {
    val inputPaths = inputs.map { case (name, recs) =>
      val p = dir.resolve(name)
      writeDat(p, recs)
      p
    }
    val before = inputPaths.map(p => p -> readBytes(p)).toMap
    val allInput = inputs.flatMap(_._2)
    val output = dir.resolve("out.dat")

    val runs = Main.sortFile(
      inputPaths.map(_.toString).toList,
      output.toString + ".tmp",
      blockSize,
      threads
    )
    val merge = new MergeSort(runs, output.toString, readBufferSize)
    merge.init()
    merge.sort()
    Main.cleanUp(runs)

    before.foreach { case (p, bytes) =>
      assert(readBytes(p) sameElements bytes, s"source modified: $p")
    }
    runs.foreach { r =>
      assert(!Files.exists(java.nio.file.Paths.get(r)), s"run not cleaned: $r")
    }

    val sorted = readDat(output)
    assert(sorted.size == allInput.size)
    assert(sameMultiset(sorted, allInput))
    assert(isNonDecreasing(sorted))
    (output, runs, sorted)
  }

  test("single-block unsorted file sorts end-to-end") {
    withTempDir { dir =>
      val records = Seq((3, 0, 0, 0), (1, 0, 0, 0), (2, 0, 0, 1), (2, 0, 0, 0))
      endToEndSort(dir, Seq("in.dat" -> records), blockSize = 64)
    }
  }

  test("multi-block tiny blocksize still produces global order") {
    withTempDir { dir =>
      // 6 records * 16 = 96 bytes; blockSize 32 => 3 batches
      val records = Seq(
        (5, 0, 0, 0), (1, 0, 0, 0), (4, 0, 0, 0),
        (2, 0, 0, 0), (6, 0, 0, 0), (3, 0, 0, 0)
      )
      val input = dir.resolve("in.dat")
      writeDat(input, records)
      val runs = Main.sortFile(List(input.toString), dir.resolve("unused.tmp").toString, blockSize = 32, maxConcurrency = 2)
      assert(runs.size == 3)
      assert(runs(0).endsWith("in.dat.0"))
      assert(runs(1).endsWith("in.dat.1"))
      assert(runs(2).endsWith("in.dat.2"))

      val output = dir.resolve("out.dat")
      val merge = new MergeSort(runs, output.toString, readBufferSize = 16)
      merge.init()
      merge.sort()
      Main.cleanUp(runs)

      val sorted = readDat(output)
      assert(sorted == records.sortBy(identity))
      assert(isNonDecreasing(sorted))
    }
  }

  test("multi-file inputs merge into one ordered output") {
    withTempDir { dir =>
      endToEndSort(
        dir,
        Seq(
          "a.dat" -> Seq((9, 0, 0, 0), (1, 0, 0, 0)),
          "b.dat" -> Seq((5, 0, 0, 0), (3, 0, 0, 0), (7, 0, 0, 0))
        ),
        blockSize = 32,
        threads = 2
      )
    }
  }

  test("source files unchanged and temp runs cleaned up") {
    withTempDir { dir =>
      endToEndSort(
        dir,
        Seq("in.dat" -> Seq((2, 0, 0, 0), (1, 0, 0, 0), (3, 0, 0, 0))),
        blockSize = 16 // one record per batch
      )
    }
  }
}
