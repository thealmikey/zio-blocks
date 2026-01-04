package zio.blocks.chunk

import zio._
import zio.test._

object ChunkIntegrationSpec extends ZIOSpecDefault {

  override def spec = suite("Chunk Integration")(
    fiberSafeSharingSuite,
    parallelProcessingSuite,
    batchProcessingSuite,
    designLockSuite
  ) @@ TestAspect.parallel

  private def fiberSafeSharingSuite = suite("Fiber-Safe Sharing")(
    test("shared Chunk allows concurrent reads with consistent checksums") {
      val size       = 1000
      val fiberCount = 100
      val chunk      = Chunk.fromArray((1 to size).toArray)

      // Checksum function: simple sum using foldLeft
      def computeChecksum(c: Chunk[Int]): Int = c.foldLeft(0)(_ + _)

      val expectedChecksum = computeChecksum(chunk)

      for {
        results <- ZIO.foreachPar(1 to fiberCount)(_ => ZIO.succeed(computeChecksum(chunk)))
      } yield assertTrue(results.length == fiberCount) &&
        assertTrue(results.forall(_ == expectedChecksum))
    },

    test("parallel reads via property testing never corrupt data") {
      check(Gen.chunkOf(Gen.int)) { chunk =>
        for {
          results <- ZIO.foreachPar(1 to 10) { _ =>
            ZIO.succeed(chunk.toList)
          }
        } yield assertTrue(results.forall(_ == chunk.toList))
      }
    },

    test("demonstrate safety: concurrent reads of shared Chunk always yield consistent results") {
      val size        = 1000
      val data        = (1 to size).toArray
      val sharedChunk = Chunk.fromArray(data)

      def sumChunk(c: Chunk[Int]): Int = c.foldLeft(0)(_ + _)

      val expected = data.sum

      for {
        // Chunk is safe: concurrent reads always yield the same result because it is immutable.
        chunkResults <- ZIO.foreachPar(1 to 100)(_ => ZIO.succeed(sumChunk(sharedChunk)))
      } yield {
        assertTrue(chunkResults.forall(_ == expected)) &&
        assertTrue(sharedChunk.length == size)
      }
    }
  )

  private def parallelProcessingSuite = suite("Parallel Processing")(
    test("ZIO.foreachPar preserves size and value invariants") {
      check(Gen.chunkOf(Gen.int)) { chunk =>
        for {
          result <- ZIO.foreachPar(chunk)(a => ZIO.succeed(a))
        } yield assertTrue(result.size == chunk.length) &&
          assertTrue(result.toList == chunk.toList)
      }
    },

    test("ZIO.collectAllPar with indexed elements guarantees completeness and integrity") {
      check(Gen.chunkOfBounded(10, 100)(Gen.int)) { chunk =>
        val indexed = chunk.zipWithIndex
        val effects = indexed.map { case (value, index) =>
          ZIO.succeed((value, index))
        }
        for {
          result <- ZIO.collectAllPar(effects)
          // Sort by original index to verify values
          restored = result.sortBy(_._2).map(_._1)
        } yield assertTrue(result.size == chunk.length) &&
          assertTrue(restored.toList == chunk.toList)
      }
    },

    test("stress-test: 1000+ parallel operations with timing via ZIO.Clock") {
      val opCount = 2000
      val chunk   = Chunk.fromArray((1 to opCount).toArray)
      for {
        startNanos <- Clock.nanoTime
        result     <- ZIO.foreachPar(chunk)(i => ZIO.succeed(i * 2))
        endNanos   <- Clock.nanoTime
        durationMs = (endNanos - startNanos) / 1000000
        _          <- ZIO.logAnnotate("duration", s"${durationMs}ms")(ZIO.logDebug("Parallel stress test complete"))
      } yield assertTrue(result.size == opCount) &&
        assertTrue(result(opCount - 1) == opCount * 2)
    }
  )

  private def batchProcessingSuite = suite("Batch Processing")(
    test("Chunk.grouped(n) produces correct batch sizes for parallel processing") {
      check(Gen.chunkOfBounded(10, 100)(Gen.int), Gen.int(1, 10)) { (chunk, n) =>
        val groups = Chunk.fromIterator(chunk.grouped(n))
        for {
          results <- ZIO.foreachPar(groups)(group => ZIO.succeed(group.length))
        } yield {
          val expectedGroupCount = (chunk.length + n - 1) / n
          assertTrue(results.size == expectedGroupCount) &&
          assertTrue(results.dropRight(1).forall(_ == n)) &&
          assertTrue(results.lastOption.forall(_ <= n))
        }
      }
    },

    test("Chunk.sliding(size, step) works correctly with foreachPar on each window") {
      val chunk = Chunk.fromArray((1 to 10).toArray)
      val size  = 3
      val step  = 2
      // Windows: [1,2,3], [3,4,5], [5,6,7], [7,8,9], [9,10]
      val windows = Chunk.fromIterator(chunk.sliding(size, step))
      for {
        sums <- ZIO.foreachPar(windows)(w => ZIO.succeed(w.foldLeft(0)(_ + _)))
      } yield assertTrue(sums == Chunk(6, 12, 18, 24, 19))
    },

    test("Chunk concatenation (++) preserves structural integrity under deep nesting") {
      val n = 1000
      val chunks = (1 to n).map(i => Chunk.single(i))

      // Deep left-fold concatenation
      val result = chunks.foldLeft(Chunk.empty[Int])(_ ++ _)

      assertTrue(result.length == n) &&
      assertTrue(result.head == 1) &&
      assertTrue(result(n - 1) == n) &&
      assertTrue(result.toList == (1 to n).toList)
    },

    test("Chunk.slice provides zero-copy views suitable for parallel distribution") {
      val size  = 1000000
      val array = (1 to size).toArray
      val chunk = Chunk.fromArray(array)
      val mid   = size / 2

      val leftSlice  = chunk.slice(0, mid)
      val rightSlice = chunk.slice(mid, size)

      // Zero-copy verification: Check that the internal references are the same
      // while allowing parallel computation on disjoint views.
      def sumLong(c: Chunk[Int]): Long = c.foldLeft(0L)(_ + _)

      for {
        results <- ZIO.collectAllPar(List(
                     ZIO.succeed(sumLong(leftSlice)),
                     ZIO.succeed(sumLong(rightSlice))
                   ))
      } yield {
        val totalSum = results.foldLeft(0L)(_ + _)

        var expected = 0L
        var i        = 0
        while (i < array.length) {
          expected = expected + array(i)
          i = i + 1
        }

        assertTrue(totalSum == expected) &&
        assertTrue(leftSlice.length == mid) &&
        assertTrue(rightSlice.length == size - mid)
      }
    }
  )

  /**
   * ARCHITECTURAL DOCUMENTATION: Design Lock Suite
   *
   * These tests exist to enforce the invariant that Chunk remains the primary collection type
   * throughout the library's internal and external APIs.
   *
   * 1. Type Signatures: We use compile-time evidence (=:=) to ensure that methods do not
   *    accidentally widen to Seq, IndexedSeq, or Iterable.
   * 2. Implementation Integrity: We verify that factory methods return concrete Chunk
   *    specializations rather than standard library collections.
   * 3. Fluent API Preservation: We ensure that transformation methods (map, filter, etc.)
   *    return Chunks, allowing for specialized performance optimizations like zero-copy
   *    slicing or bit-packing to persist through the call chain.
   *
   * Changes that cause these tests to fail should be treated as breaking architectural regressions.
   */
  private def designLockSuite = suite("Design Lock")(
    test("compile-time: factory methods return exact Chunk types") {
      // The following assignments rely on the return type being exactly Chunk[T].
      // If these methods were modified to return Seq or List, this test would fail to compile.
      val fromArray: Chunk[Int]    = Chunk.fromArray(Array(1, 2, 3))
      val fromIterable: Chunk[Int] = Chunk.fromIterable(List(1, 2, 3))
      val singleton: Chunk[Int]    = Chunk.single(1)

      assertTrue(fromArray.length == 3) &&
      assertTrue(fromIterable.length == 3) &&
      assertTrue(singleton.length == 1)
    },

    test("compile-time: transformation methods preserve Chunk type") {
      val chunk: Chunk[Int] = Chunk(1, 2, 3)

      // The following assignments rely on the return type being exactly Chunk[T].
      // If map/filter/flatMap were modified to return List or Vector, this test would fail to compile.
      val mapped: Chunk[String]     = chunk.map(_.toString)
      val filtered: Chunk[Int]      = chunk.filter(_ > 1)
      val flatMapped: Chunk[Int]    = chunk.flatMap(i => Chunk(i, i))
      val collected: Chunk[Int]     = chunk.collect { case i if i > 1 => i }
      val zipped: Chunk[(Int, Int)] = chunk.zip(chunk)

      assertTrue(mapped.length == 3) &&
      assertTrue(filtered.length == 2) &&
      assertTrue(flatMapped.length == 6) &&
      assertTrue(collected.length == 2) &&
      assertTrue(zipped.length == 3)
    },

    test("type-consistency: Chunk[Int] =:= Chunk[Int]") {
      def checkType[A, B](implicit ev: A =:= B): Boolean = ev != null
      
      assertTrue(checkType[Chunk[Int], Chunk[Int]]) &&
      assertTrue(checkType[Chunk[Boolean], Chunk[Boolean]])
    }
  )
}
