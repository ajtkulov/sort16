package sort16.support

import sort16.RecordCompare

import java.nio.file.{Files, Path}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer

object RecordIo {
  type Record = (Int, Int, Int, Int)

  def pack(i0: Int, i1: Int, i2: Int, i3: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
    buf.putInt(i0).putInt(i1).putInt(i2).putInt(i3)
    buf.array()
  }

  def pack(r: Record): Array[Byte] = pack(r._1, r._2, r._3, r._4)

  def unpack(bytes: Array[Byte], offset: Int = 0): Record = {
    val buf = ByteBuffer.wrap(bytes, offset, 16).order(ByteOrder.BIG_ENDIAN)
    (buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt())
  }

  def writeDat(path: Path, records: Seq[Record]): Unit = {
    val out = Files.newOutputStream(path)
    try {
      records.foreach(r => out.write(pack(r)))
    } finally {
      out.close()
    }
  }

  def writeRaw(path: Path, bytes: Array[Byte]): Unit = {
    Files.write(path, bytes)
  }

  def readDat(path: Path): Vector[Record] = {
    val bytes = Files.readAllBytes(path)
    require(bytes.length % 16 == 0, s"length ${bytes.length} not divisible by 16")
    val result = ArrayBuffer.empty[Record]
    var i = 0
    while (i < bytes.length) {
      result += unpack(bytes, i)
      i += 16
    }
    result.toVector
  }

  def readBytes(path: Path): Array[Byte] = Files.readAllBytes(path)

  def isNonDecreasing(records: Seq[Record]): Boolean = {
    records.sliding(2).forall {
      case Seq(a, b) =>
        RecordCompare.compare(pack(a), 0, pack(b), 0) <= 0
      case _ => true
    }
  }

  def sameMultiset(a: Seq[Record], b: Seq[Record]): Boolean =
    a.sorted == b.sorted

  def withTempDir[A](f: Path => A): A = {
    val dir = Files.createTempDirectory("sort16-test-")
    try {
      f(dir)
    } finally {
      deleteRecursively(dir)
    }
  }

  def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val stream = Files.list(path)
      try {
        stream.forEach(p => deleteRecursively(p))
      } finally {
        stream.close()
      }
    }
    Files.deleteIfExists(path)
  }
}
