package sort16

import org.scalatest.funsuite.AnyFunSuite
import sort16.support.RecordIo._

class RecordFormatSpec extends AnyFunSuite {

  private def cmp(a: Record, b: Record): Int =
    RecordCompare.compare(pack(a), 0, pack(b), 0)

  test("orders by first differing integer at index 0") {
    assert(cmp((1, 0, 0, 0), (2, 0, 0, 0)) < 0)
    assert(cmp((2, 0, 0, 0), (1, 0, 0, 0)) > 0)
  }

  test("orders by first differing integer at index 1") {
    assert(cmp((0, 1, 9, 9), (0, 2, 0, 0)) < 0)
    assert(cmp((0, 2, 0, 0), (0, 1, 9, 9)) > 0)
  }

  test("orders by first differing integer at index 2") {
    assert(cmp((0, 0, 1, 9), (0, 0, 2, 0)) < 0)
    assert(cmp((0, 0, 2, 0), (0, 0, 1, 9)) > 0)
  }

  test("orders by first differing integer at index 3") {
    assert(cmp((0, 0, 0, 1), (0, 0, 0, 2)) < 0)
    assert(cmp((0, 0, 0, 2), (0, 0, 0, 1)) > 0)
  }

  test("equal records compare as zero") {
    assert(cmp((1, 2, 3, 4), (1, 2, 3, 4)) == 0)
  }

  test("signed interpretation of high-bit integers") {
    // 0xFFFFFFFF as signed int is -1, which orders before 0
    assert(cmp((-1, 0, 0, 0), (0, 0, 0, 0)) < 0)
    assert(cmp((0, 0, 0, 0), (-1, 0, 0, 0)) > 0)
    assert(cmp((Int.MinValue, 0, 0, 0), (Int.MaxValue, 0, 0, 0)) < 0)
  }

  test("RecordWrap.ordering agrees with RecordCompare (max-heap negation)") {
    val a = pack(1, 0, 0, 0)
    val b = pack(2, 0, 0, 0)
    val wa = RecordWrap(a, 0, runIndex = 0)
    val wb = RecordWrap(b, 0, runIndex = 1)
    assert(RecordWrap.ordering.compare(wa, wb) == -RecordCompare.compare(a, 0, b, 0))
  }
}
