package sort16

import sort.FileUtils
import java.io._
import java.io.FileOutputStream
import org.rogach.scallop._


case class Batch(file: RandomAccessFile, offset: Long, outputFileName: String, idx: Int, blockSize: Int) {
  var len = blockSize
  @volatile private var buffer: Array[Byte] = null
  @volatile private var newAr: Array[Int] = null

  private var bytesRead: Int = 0
  private var itemsCount: Int = 0

  def outputFile(): String = {
    s"$outputFileName.$idx"
  }

  def read(): Unit = {
    buffer = new Array[Byte](blockSize)
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
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFile()), 10485760)

    for {
      idx <- 0 until itemsCount
    } {
      outputStream.write(buffer, newAr(idx) * 16, 16)
    }
    outputStream.close()
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

class FileIterator(val fileName: String, val offset: Int = 0, val bufferSize: Int = 20000000, val index: Int) {
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

class MergeSort(sortedFiles: Vector[String], outputFileName: String) {
  val outputStream = new BufferedOutputStream(new FileOutputStream(outputFileName), 10485760)
  val heap = scala.collection.mutable.PriorityQueue[RecordWrap]()(RecordWrap.ordering)
  val fileMap = new Array[FileIterator](sortedFiles.size)

  def init(): Unit = {
    print("merge sort")
    sortedFiles.zipWithIndex.foreach { case (f, idx) =>
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
  val files = trailArg[List[String]]()
  val output = opt[String](required = true)
  val blocksize = opt[Int]()
  val threads = opt[Int]()
  verify()
}

object Main extends App {
  def sortFile(files: List[String], outputFileName: String, blockSize: Int, maxConcurrency: Int = 12): Vector[String] = {
    val batches = (for {fileName <- files
                        size: Long = FileUtils.fileSize(fileName)
                        idx <- 0 to ((size - 1) / blockSize).toInt
                        } yield {
      Batch(new RandomAccessFile(fileName, "r"), blockSize * idx, fileName, 0, blockSize)
    }).toVector.zipWithIndex.map { case (b, idx) => b.copy(idx = idx) }

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

    batches.map(_.outputFile())
  }

  def cleanUp(filesToDelete: Vector[String]) = {
    filesToDelete.foreach(file => FileUtils.delete(file))
  }

  override def main(args: Array[String]) {
    val conf = new Conf(args)

    val blockSize: Int = conf.blocksize.getOrElse(1000000000).toInt
    val maxConcurrency: Int = conf.threads.getOrElse(12).toInt
    val files: List[String] = conf.files.get.get
    val output = conf.output.get.get

    println(s"params, files=${files.mkString(",")}, blockSize=${blockSize}, threads=${maxConcurrency}, output=$output")

    val chunks: Vector[String] = sortFile(files, s"$output.tmp", blockSize, maxConcurrency)

    val m = new MergeSort(chunks, output)
    m.init()
    m.sort()
    cleanUp(chunks)
  }
}
