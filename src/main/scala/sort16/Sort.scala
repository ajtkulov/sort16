package sort16

import sort.FileUtils
import java.io._
import java.io.FileOutputStream
import org.rogach.scallop._


case class Batch(file: RandomAccessFile, offset: Long, outputFileName: String, idx: Int, blockSize: Int) {
  var len = blockSize
  @volatile private var buffer = new Array[Byte](blockSize)
  @volatile private var newAr: Array[Int] = null

  private var bytesRead: Int = 0
  private var itemsCount: Int = 0

  def read(): Unit = {
    file.seek(offset)
    bytesRead = file.read(buffer)
    assert(bytesRead % 16 == 0, bytesRead)
    itemsCount = bytesRead / 16
  }

  def internalSort(): Unit = {
    newAr = Array.range(0, itemsCount).sortWith { case (l, r) =>
      var idx = 0
      val ll = l * 16
      val rr = r * 16
      while (idx < 15 && buffer(ll + idx) == buffer(rr + idx)) {
        idx = idx + 1
      }

      (buffer(ll + idx) compareTo buffer(rr + idx)) < 0
    }
  }

  def write(): Unit = {
    val fos = new FileOutputStream(s"$outputFileName.$idx")
    for {
      idx <- 0 until itemsCount
    } {
      fos.write(buffer, newAr(idx) * 16, 16)
    }
    fos.close()
  }

  def customFinalize(): Unit = {
    file.close()
    buffer = null
  }
}

case class RecordWrap(ar: Array[Byte], offset: Int, isLastInBlock: Boolean, index: Int)

object RecordWrap {
  val ordering = new Ordering[RecordWrap] {
    override def compare(x: RecordWrap, y: RecordWrap): Int = {
      var idx = 0

      while (idx < 15 && x.ar(x.offset + idx) == y.ar(y.offset + idx)) {
        idx = idx + 1
      }

      x.ar(x.offset + idx) compareTo y.ar(y.offset + idx)
    }
  }
}

class FileIterator(val fileName: String, val offset: Int = 0, val bufferSize: Int = 10000000, val index: Int) {
  val size = FileUtils.fileSize(fileName)
  val file = new RandomAccessFile(fileName, "r")
  file.seek(offset)
  var buffer: Array[Byte] = new Array[Byte](bufferSize)
  val bytesRead = file.read(buffer)
  val isReadTillEnd: Boolean = offset.toLong + bytesRead.toLong == size

  def nextChunk(): Iterator[RecordWrap] = {
    val lastIdx = bytesRead / 16
    val range = 0 until lastIdx

    range.iterator.map {
      idx => RecordWrap(buffer, idx * 16, idx == lastIdx - 1, index)
    }
  }
}

class MergeSort(inputFileName: String, chunks: Int, outputFileName: String) {
  val outputStream = new BufferedOutputStream(new FileOutputStream(outputFileName), 65536)
  val heap = scala.collection.mutable.PriorityQueue[RecordWrap]()(RecordWrap.ordering)
  val files = (0 until chunks).map(x => s"$inputFileName.$x").toVector
  val fileMap = new Array[FileIterator](chunks)

  def init(): Unit = {
    print("merge sort")
    files.zipWithIndex.foreach { case (f, idx) =>
      fileMap(idx) = new FileIterator(f, 0, index = idx)
      val chunks = fileMap(idx).nextChunk()
      chunks.foreach { str =>
        heap.enqueue(str)
      }
    }
  }

  def sort(): Unit = {
    while (heap.nonEmpty) {
      val head = heap.dequeue()

      if (head.isLastInBlock) {
        val idx = head.index
        if (fileMap(idx) != null) {
          if (!fileMap(idx).isReadTillEnd) {
            val curFileIterator = fileMap(idx)
            fileMap(idx) = new FileIterator(curFileIterator.fileName, curFileIterator.offset + curFileIterator.bytesRead, curFileIterator.bufferSize, idx)
            val chunks = fileMap(idx).nextChunk()

            chunks.foreach { str =>
              heap.enqueue(str)
            }
          } else {
            fileMap(idx) = null
          }
        }
      }

      outputStream.write(head.ar, head.offset, 16)
    }

    outputStream.close()
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = true)
  val output = opt[String](required = true)
  val blocksize = opt[Int]()
  val threads = opt[Int]()
  verify()
}

object Main extends App {
  def sortFile(inputFile: String, blockSize: Int, maxConcurrency: Int = 12): Int = {
    val size: Long = FileUtils.fileSize(inputFile)

    val batches = (0 to ((size - 1) / blockSize).toInt).map { idx =>
      Batch(new RandomAccessFile(inputFile, "r"), blockSize * idx, inputFile, idx.toInt, blockSize)
    }

    import zio._

    val processBatch = (b: Batch) => for {
      _ <- ZIO.attempt(b.read())
      _ <- ZIO.attempt(b.internalSort())
      _ <- ZIO.attempt(b.write())
      _ <- ZIO.attempt(b.customFinalize())
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

    batches.size
  }

  def cleanUp(filesToDelete: Vector[String]) = {
    filesToDelete.foreach(file => FileUtils.delete(file))
  }

  override def main(args: Array[String]) {
    val conf = new Conf(args)

    val blockSize: Int = conf.blocksize.getOrElse(1000000000).toInt
    val maxConcurrency: Int = conf.threads.getOrElse(12).toInt
    val input = conf.input.get.get
    val output = conf.output.get.get

    val chunks: Int = sortFile(input, blockSize, maxConcurrency)

    val m = new MergeSort(input, chunks, output)
    m.init()
    m.sort()
    cleanUp(m.files)
  }
}
