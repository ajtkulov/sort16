package sort16

import org.scalatest.funsuite.AnyFunSuite
import sort16.support.RecordIo._

import java.io.RandomAccessFile

class InvalidRecordLengthSpec extends AnyFunSuite {

  test("Batch.read fails when file length is not a multiple of 16") {
    withTempDir { dir =>
      val input = dir.resolve("bad.dat")
      writeRaw(input, Array[Byte](1, 2, 3, 4, 5)) // 5 bytes

      val batch = new Batch(
        new RandomAccessFile(input.toFile, "r"),
        offset = 0L,
        outputFileName = input.toString,
        idx = 0,
        blockSize = 64
      )

      assertThrows[AssertionError] {
        batch.read()
      }
      batch.customFinalize()
    }
  }

  test("RunReader.load fails when file length is not a multiple of 16") {
    withTempDir { dir =>
      val input = dir.resolve("bad.dat")
      writeRaw(input, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)) // 15 bytes

      val reader = new RunReader(input.toString, fileOffset = 0L, bufferSize = 64, index = 0)
      assertThrows[AssertionError] {
        reader.load()
      }
    }
  }
}
