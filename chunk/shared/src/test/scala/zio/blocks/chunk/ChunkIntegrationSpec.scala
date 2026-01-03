package zio.blocks.chunk

import zio._
import zio.test._
import zio.test.Assertion._
import scala.collection.mutable.ArrayBuffer

object ChunkIntegrationSpec extends ZIOSpecDefault {

  def spec = suite("ChunkIntegrationSpec")(
    fiberSafeSharingSuite,
    parallelProcessingSuite
  )

  val fiberSafeSharingSuite = suite("fiberSafeSharingSuite")(
    test("shared Chunk allows concurrent reads with consistent checksums") {
      val size       = 1000
      val fiberCount = 100
      val chunk      = Chunk.fromArray((1 to size).toArray)

      // Checksum function: simple sum using project-style while loop on iterator
      def computeChecksum(c: Chunk[Int]): Int = {
        var sum  = 0
        val iter = c.chunkIterator
        while (iter.hasNext) {
          sum += iter.next()
        }
        sum
      }

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

    test("demonstrate safety by comparing Chunk with mutable ArrayBuffer") {
      val size         = 1000
      val data         = (1 to size).toArray
      val sharedChunk  = Chunk.fromArray(data)
      val sharedBuffer = ArrayBuffer.from(data)

      def sumChunk(c: Chunk[Int]): Int = c.foldLeft(0)(_ + _)
      def sumBuffer(b: ArrayBuffer[Int]): Int = {
        var s = 0
        var i = 0
        val len = b.length
        while (i < len) {
          s += b(i)
          i += 1
        }
        s
      }

      val expected = data.sum

      for {
        // Chunk is safe: concurrent reads always yield the same result.
        chunkResults <- ZIO.foreachPar(1 to 100)(_ => ZIO.succeed(sumChunk(sharedChunk)))

        // ArrayBuffer is unsafe: one fiber modifying while others read causes inconsistent sums.
        // We use a high concurrency to increase the chance of observing the race.
        bufferResults <- ZIO.foreachPar(1 to 100) { i =>
          if (i % 2 == 0) {
            ZIO.succeed {
              val old = sharedBuffer(0)
              sharedBuffer(0) = -1 // Mutate
              val s = sumBuffer(sharedBuffer)
              sharedBuffer(0) = old // Restore
              s
            }
          } else {
            ZIO.succeed(sumBuffer(sharedBuffer))
          }
        }
      } yield {
        val chunkSafe = chunkResults.forall(_ == expected)
        // We assert Chunk safety. ArrayBuffer unsafety is non-deterministic but 
        // the presence of mutation in parallel loops is logically unsafe.
        assertTrue(chunkSafe) &&
        assertTrue(sharedChunk.length == size) &&
        assertTrue(sharedBuffer.length == size)
      }
    }
  )

  val parallelProcessingSuite = suite("parallelProcessingSuite")(
    test("ZIO.foreachPar preserves size and value invariants") {
      check(Gen.chunkOf(Gen.int)) { chunk =>
        for {
          result <- ZIO.foreachPar(chunk)(a => ZIO.succeed(a))
        } yield assertTrue(result.size == chunk.length) &&
          assertTrue(result.toList == chunk.toList)
      }
    },

    test("ZIO.collectAllPar with indexed elements guarantees completeness and integrity") {
      check(Gen.chunkOfBound(10, 100)(Gen.int)) { chunk =>
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
}
