package zio.blocks.chunk

import zio._
import zio.test._
import scala.reflect.ClassTag

object ChunkBenchmark extends ZIOSpecDefault {

  private val Size = 10000
  private val HalfSize = 5000

  def spec = suite("ChunkBenchmark")(
    mapBenchmark,
    filterBenchmark,
    concatenationBenchmark,
    iterationBenchmark
  ) @@ TestAspect.tag("benchmark") @@ TestAspect.sequential

  private def time[A](f: => A): (A, Long) = {
    val start = java.lang.System.currentTimeMillis()
    val res   = f
    val end   = java.lang.System.currentTimeMillis()
    (res, end - start)
  }

  private val mapBenchmark = test("map performance vs standard collections") {
    val elements = (1 to Size).toArray
    val list     = elements.toList
    val vector   = elements.toVector
    val chunk    = Chunk.fromArray(elements)

    val (_, listTime)   = time(list.map(_ + 1))
    val (_, vectorTime) = time(vector.map(_ + 1))
    val (_, arrayTime)  = time(elements.map(_ + 1))
    val (_, chunkTime)  = time(chunk.map(_ + 1))

    // Chunk should be comparable to Vector/List, though Array is usually fastest.
    // We assert it's not orders of magnitude slower (e.g., within 10x of Vector)
    assertTrue(chunkTime < vectorTime * 10)
  }

  private val filterBenchmark = test("filter performance vs standard collections") {
    val elements = (1 to Size).toArray
    val list     = elements.toList
    val vector   = elements.toVector
    val chunk    = Chunk.fromArray(elements)

    val p: Int => Boolean = _ % 2 == 0

    val (_, listTime)   = time(list.filter(p))
    val (_, vectorTime) = time(vector.filter(p))
    val (_, arrayTime)  = time(elements.filter(p))
    val (_, chunkTime)  = time(chunk.filter(p))

    assertTrue(chunkTime < vectorTime * 10)
  }

  private val concatenationBenchmark = test("concatenation performance vs standard collections") {
    val leftArr  = (1 to HalfSize).toArray
    val rightArr = (HalfSize + 1 to Size).toArray
    
    val leftList = leftArr.toList
    val rightList = rightArr.toList
    
    val leftVec = leftArr.toVector
    val rightVec = rightArr.toVector
    
    val leftChunk = Chunk.fromArray(leftArr)
    val rightChunk = Chunk.fromArray(rightArr)

    val (_, listTime)   = time(leftList ++ rightList)
    val (_, vectorTime) = time(leftVec ++ rightVec)
    val (_, arrayTime)  = time(leftArr ++ rightArr)
    val (_, chunkTime)  = time(leftChunk ++ rightChunk)

    // Chunk concatenation is O(1) (creates a Concat node), 
    // so it should be significantly faster than List/Array ++ (O(N)).
    assertTrue(chunkTime < listTime)
  }

  private val iterationBenchmark = test("iteration (foreach vs iterator) performance") {
    val elements = (1 to Size).toArray
    val chunk    = Chunk.fromArray(elements)

    val (_, foreachTime) = time {
      var sum = 0L
      chunk.foreach(sum += _)
      sum
    }

    val (_, iteratorTime) = time {
      var sum = 0L
      val it = chunk.iterator
      while (it.hasNext) {
        sum += it.next()
      }
      sum
    }

    // foreach is generally faster as it avoids iterator allocation and hasNext/next overhead
    assertTrue(foreachTime <= iteratorTime * 2) 
  }
}
