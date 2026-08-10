package sort16

import sort.FileUtils

import java.io._
import org.rogach.scallop._

import java.lang.System.gc

object RecordCompare {
  /** Read a signed big-endian 32-bit int without allocating. */
  def readIntBE(bytes: Array[Byte], offset: Int): Int = {
    ((bytes(offset) & 0xff) << 24) |
      ((bytes(offset + 1) & 0xff) << 16) |
      ((bytes(offset + 2) & 0xff) << 8) |
      (bytes(offset + 3) & 0xff)
  }

  /** Natural order: negative if left < right, using four signed big-endian ints. */
  def compare(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int = {
    var i = 0
    while (i < 4) {
      val l = readIntBE(left, leftOffset + i * 4)
      val r = readIntBE(right, rightOffset + i * 4)
      if (l != r) return Integer.compare(l, r)
      i += 1
    }
    0
  }
}

/** Mutable batch pipeline: read → index-sort → contiguous write. */
class Batch(
    private val file: RandomAccessFile,
    private val offset: Long,
    private val outputFileName: String,
    val idx: Int,
    private val blockSize: Int
) {
  private var buffer: Array[Byte] = null
  private var indices: Array[Int] = null
  private var bytesRead: Int = 0
  private var itemsCount: Int = 0

  def outputFile(): String = s"$outputFileName.$idx"

  def read(): Unit = {
    buffer = new Array[Byte](blockSize)
    file.seek(offset)
    bytesRead = file.read(buffer)
    assert(bytesRead % 16 == 0, bytesRead)
    itemsCount = bytesRead / 16
  }

  def internalSort(): Unit = {
    indices = Array.range(0, itemsCount).sortWith { case (l, r) =>
      RecordCompare.compare(buffer, l * 16, buffer, r * 16) < 0
    }
  }

  def write(): Unit = {
    val sorted = new Array[Byte](itemsCount * 16)
    var i = 0
    while (i < itemsCount) {
      System.arraycopy(buffer, indices(i) * 16, sorted, i * 16, 16)
      i += 1
    }
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFile()), 10485760)
    try {
      outputStream.write(sorted)
    } finally {
      outputStream.close()
    }
  }

  def customFinalize(): Unit = {
    file.close()
    buffer = null
    indices = null
  }

  def pipeline(): Unit = {
    read()
    internalSort()
    write()
    customFinalize()
    gc()
  }
}

/** Heap entry pointing into a run's current read buffer. */
case class RecordWrap(ar: Array[Byte], offset: Int, runIndex: Int)

object RecordWrap {
  val ordering = new Ordering[RecordWrap] {
    override def compare(x: RecordWrap, y: RecordWrap): Int = {
      // PriorityQueue is a max-heap; negate so the smallest record dequeues first.
      -RecordCompare.compare(x.ar, x.offset, y.ar, y.offset)
    }
  }
}

/**
 * Sequential reader for one sorted run: large buffered reads, cursor over records.
 * Only the current head is placed on the merge heap.
 */
class RunReader(val fileName: String, private var fileOffset: Long, val bufferSize: Int, val index: Int) {
  val size: Long = FileUtils.fileSize(fileName)

  private var buffer: Array[Byte] = null
  private var bytesInBuffer: Int = 0
  private var recordIndex: Int = 0
  private var recordCount: Int = 0

  def currentBuffer: Array[Byte] = buffer

  def currentOffset: Int = recordIndex * 16

  def hasCurrent: Boolean = recordIndex < recordCount

  /** Load next chunk from disk. Returns true if at least one record is available. */
  def load(): Boolean = {
    if (fileOffset >= size) {
      buffer = null
      bytesInBuffer = 0
      recordIndex = 0
      recordCount = 0
      return false
    }
    val file = new RandomAccessFile(fileName, "r")
    try {
      buffer = new Array[Byte](bufferSize)
      file.seek(fileOffset)
      bytesInBuffer = file.read(buffer)
      assert(bytesInBuffer % 16 == 0, bytesInBuffer)
      recordCount = bytesInBuffer / 16
      recordIndex = 0
      fileOffset += bytesInBuffer
      recordCount > 0
    } finally {
      file.close()
    }
  }

  def advance(): Unit = {
    recordIndex += 1
  }
}

class MergeSort(sortedFiles: Vector[String], outputFileName: String, readBufferSize: Int = 20000000) {
  private val outputStream = new BufferedOutputStream(new FileOutputStream(outputFileName), 10485760)
  private val heap = scala.collection.mutable.PriorityQueue[RecordWrap]()(RecordWrap.ordering)
  private val readers = new Array[RunReader](sortedFiles.size)

  def init(): Unit = {
    sortedFiles.zipWithIndex.foreach { case (f, idx) =>
      val reader = new RunReader(f, 0L, readBufferSize, idx)
      readers(idx) = reader
      if (reader.load()) {
        enqueueHead(reader)
      }
    }
  }

  private def enqueueHead(reader: RunReader): Unit = {
    heap.enqueue(RecordWrap(reader.currentBuffer, reader.currentOffset, reader.index))
  }

  def sort(): Unit = {
    try {
      while (heap.nonEmpty) {
        val head = heap.dequeue()
        outputStream.write(head.ar, head.offset, 16)

        val reader = readers(head.runIndex)
        reader.advance()
        if (reader.hasCurrent) {
          enqueueHead(reader)
        } else if (reader.load()) {
          enqueueHead(reader)
        }
      }
    } finally {
      outputStream.close()
    }
  }
}

class Conf(arguments: Seq[String], throwOnError: Boolean = false) extends ScallopConf(arguments) {
  val files = trailArg[List[String]]()
  val output = opt[String](required = true)
  val blocksize = opt[Int]()
  val threads = opt[Int]()
  val readbuffersize = opt[Int]()
  val action = opt[String]()

  override def onError(e: Throwable): Unit = {
    if (throwOnError) throw e
    else super.onError(e)
  }

  verify()
}

object Main {
  def sortFile(files: List[String], outputFileName: String, blockSize: Int, maxConcurrency: Int = 12): Vector[String] = {
    val batches: Vector[Batch] = (for {
      fileName <- files
      size = FileUtils.fileSize(fileName)
      blockIdx <- 0 to ((size - 1) / blockSize).toInt
    } yield (fileName, blockSize.toLong * blockIdx)).toVector.zipWithIndex.map {
      case ((fileName, offset), idx) =>
        new Batch(new RandomAccessFile(fileName, "r"), offset, fileName, idx, blockSize)
    }

    import zio._

    val processBatch = (b: Batch) => for {
      _ <- ZIO.attempt(b.pipeline())
    } yield ()

    val semaphore = zio.Semaphore.make(maxConcurrency)
    val parallelProcessing = semaphore.flatMap { sem =>
      ZIO.foreachPar(batches) { batch =>
        sem.withPermit(processBatch(batch))
      }
    }

    zio.Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe.run(parallelProcessing).getOrThrow()
    }

    batches.map(_.outputFile())
  }

  def cleanUp(filesToDelete: Vector[String]): Unit = {
    filesToDelete.foreach(file => FileUtils.delete(file))
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val blockSize: Int = conf.blocksize.getOrElse(1000000000)
    val maxConcurrency: Int = conf.threads.getOrElse(12)
    val files: List[String] = conf.files.get.get
    val output = conf.output.get.get
    val action = conf.action.getOrElse("sort")
    val readBufferSize = conf.readbuffersize.getOrElse(20000000)

    println(s"params, files=${files.mkString(",")}, blockSize=${blockSize}, threads=${maxConcurrency}, output=$output")

    if (action == "sort") {
      val chunks: Vector[String] = sortFile(files, s"$output.tmp", blockSize, maxConcurrency)

      val m = new MergeSort(chunks, output, readBufferSize)
      m.init()
      m.sort()
      cleanUp(chunks)
    } else {
      val m = new MergeSort(files.toVector, output, readBufferSize)
      m.init()
      m.sort()
    }
  }
}
