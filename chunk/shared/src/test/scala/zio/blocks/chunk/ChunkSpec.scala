package zio.blocks.chunk

import zio.test._
import zio.test.Assertion._
import zio._
import scala.reflect.ClassTag

object ChunkSpec extends ZIOSpecDefault {
  def spec = suite("ChunkSpec")(
    basicOperationsSuite,
    specializedArraySuite,
    concatenationSuite,
    transformationSuite,
    bitwiseSuite,
    threadSafetySuite,
    edgeCasesSuite
  )

  private val basicOperationsSuite = suite("Basic Operations")(
    test("apply and length") {
      val chunk = Chunk(1, 2, 3)
      assertTrue(
        chunk.length == 3,
        chunk(0) == 1,
        chunk(1) == 2,
        chunk(2) == 3
      )
    },
    test("isEmpty / isNotEmpty") {
      assertTrue(
        Chunk.empty.isEmpty,
        Chunk(1).isNotEmpty,
        !Chunk(1).isEmpty
      )
    },
    test("head / headOption / lastOption") {
      val chunk = Chunk(1, 2, 3)
      assertTrue(
        chunk.head == 1,
        chunk.headOption == Some(1),
        chunk.lastOption == Some(3),
        Chunk.empty.headOption == None
      )
    },
    test("conversions (toArray, toList, toVector)") {
      val chunk = Chunk(1, 2, 3)
      assertTrue(
        chunk.toArray.sameElements(Array(1, 2, 3)),
        chunk.toList == List(1, 2, 3),
        chunk.toVector == Vector(1, 2, 3)
      )
    }
  )

  private val specializedArraySuite = suite("Specialized Array Performance")(
    test("specialized primitive chunks") {
      def check[A: ClassTag](as: Array[A], expectedClass: String) = {
        val chunk = Chunk.fromArray(as)
        val className = chunk.getClass.getName
        // Use getName to be more robust across platforms (JS/Native)
        assertTrue(className.contains(expectedClass))
      }

      check(Array[Byte](1), "ByteArray") &&
      check(Array[Short](1), "ShortArray") &&
      check(Array[Int](1), "IntArray") &&
      check(Array[Long](1), "LongArray") &&
      check(Array[Float](1.0f), "FloatArray") &&
      check(Array[Double](1.0), "DoubleArray") &&
      check(Array[Boolean](true), "BooleanArray") &&
      check(Array[Char]('a'), "CharArray")
    },
    test("reference types use Arr") {
      val chunk = Chunk.fromArray(Array("a", "b"))
      assertTrue(chunk.getClass.getSimpleName.contains("Arr"))
    }
  )

  private val concatenationSuite = suite("Concatenation Balancing")(
    test("basic ++") {
      val left = Chunk(1, 2)
      val right = Chunk(3, 4)
      assertTrue((left ++ right).toList == List(1, 2, 3, 4))
    },
    test("empty concatenation") {
      val chunk = Chunk(1, 2)
      assertTrue(
        (chunk ++ Chunk.empty) == chunk,
        (Chunk.empty ++ chunk) == chunk
      )
    },
    test("materialization on MaxDepth") {
      val max = Chunk.MaxDepthBeforeMaterialize
      var deep = Chunk.single(0)
      for (i <- 1 to max + 1) {
        deep = deep ++ Chunk.single(i)
      }
      // After depth exceeded, it should not be a Concat node anymore (it materializes to Arr/Primitive)
      // or at least its depth should be reset/reduced via toArray in ++ implementation
      assertTrue(deep.depth <= max)
    },
    test("depth calculation") {
      val c1 = Chunk(1)
      val c2 = Chunk(2)
      val c3 = c1 ++ c2
      val c4 = c3 ++ Chunk(3)
      assertTrue(c3.depth == 1, c4.depth == 2)
    }
  )

  private val transformationSuite = suite("Transformation Methods")(
    test("map") {
      assertTrue(Chunk(1, 2, 3).map(_ * 2).toList == List(2, 4, 6))
    },
    test("filter") {
      assertTrue(Chunk(1, 2, 3, 4).filter(_ % 2 == 0).toList == List(2, 4))
    },
    test("flatMap") {
      assertTrue(Chunk(1, 2).flatMap(i => Chunk(i, i)).toList == List(1, 1, 2, 2))
    },
    test("folds") {
      val chunk = Chunk(1, 2, 3)
      assertTrue(
        chunk.foldLeft(0)(_ + _) == 6,
        chunk.foldRight(0)(_ + _) == 6
      )
    },
    test("collect / collectWhile") {
      val chunk = Chunk(1, 2, 3, 4)
      val pf: PartialFunction[Int, Int] = { case i if i % 2 == 0 => i * 10 }
      assertTrue(
        chunk.collect(pf).toList == List(20, 40),
        chunk.collectWhile { case i if i < 3 => i }.toList == List(1, 2)
      )
    },
    test("take / drop / takeWhile / dropWhile") {
      val chunk = Chunk(1, 2, 3, 4)
      assertTrue(
        chunk.take(2).toList == List(1, 2),
        chunk.drop(2).toList == List(3, 4),
        chunk.takeWhile(_ < 3).toList == List(1, 2),
        chunk.dropWhile(_ < 3).toList == List(3, 4)
      )
    },
    test("zip / zipWith / zipWithIndex") {
      val c1 = Chunk(1, 2)
      val c2 = Chunk("a", "b")
      assertTrue(
        c1.zip(c2).toList == List((1, "a"), (2, "b")),
        c1.zipWith(c2)((i, s) => s + i).toList == List("a1", "b2"),
        c1.zipWithIndex.toList == List((1, 0), (2, 1))
      )
    }
  )

  private val bitwiseSuite = suite("Bitwise Operations")(
    test("logical operations") {
      val b1 = Chunk(true, true, false, false)
      val b2 = Chunk(true, false, true, false)
      assertTrue(
        (b1 & b2).toList == List(true, false, false, false),
        (b1 | b2).toList == List(true, true, true, false),
        (b1 ^ b2).toList == List(false, true, true, false),
        b1.negate.toList == List(false, false, true, true)
      )
    },
    test("packed representation") {
      val bits = Chunk.fill(100)(true)
      val packed = bits.toPackedLong
      // 100 bits fit into 2 Longs (64 bits each)
      assertTrue(packed.length == 2)
    },
    test("asBits roundtrip") {
      val bits = Chunk(true, false, true, true, false, false, true, true)
      assertTrue(
        bits.asBitsByte.toList == bits.toList,
        bits.asBitsInt.toList == bits.toList,
        bits.asBitsLong.toList == bits.toList
      )
    },
    test("toPacked operations") {
      val bits = Chunk.fill(64)(true)
      assertTrue(
        bits.toPackedByte.length == 8,
        bits.toPackedInt.length == 2,
        bits.toPackedLong.length == 1
      )
    }
  )

  private val threadSafetySuite = suite("Thread-Safety")(
    test("concurrent appends are safe") {
      for {
        results <- ZIO.foreachPar(1 to 1000) { i =>
          ZIO.succeed {
            val base = Chunk(1, 2, 3)
            base ++ Chunk.single(i)
          }
        }
      } yield assertTrue(results.forall(_.length == 4))
    }
  )

  private val edgeCasesSuite = suite("Edge Cases")(
    test("empty chunk") {
      val empty = Chunk.empty[Int]
      val headResult = scala.util.Try(empty.head)
      assert(empty.headOption)(isNone) &&
      assertTrue(empty.length == 0) &&
      assertTrue(headResult.isFailure)
    },
    test("single element") {
      val s = Chunk.single(42)
      assertTrue(s.length == 1, s.head == 42, s(0) == 42)
    },
    test("large chunks") {
      val n = 100000
      val large = Chunk.fromArray((1 to n).toArray)
      assertTrue(large.length == n, large(n - 1) == n, large.foldLeft(0L)(_ + _) == n.toLong * (n + 1) / 2)
    },
    test("slice zero-copy semantics") {
      val chunk = Chunk(1, 2, 3, 4, 5)
      val sliced = chunk.slice(1, 4)
      assertTrue(
        sliced.toList == List(2, 3, 4),
        sliced.getClass.getSimpleName.contains("Slice")
      )
    },
    test("equals and hashCode consistency") {
      val c1 = Chunk(1, 2, 3)
      val c2 = Chunk(1, 2, 3)
      val c3 = Chunk(1, 2, 4)
      assertTrue(
        c1 == c2,
        c1.hashCode == c2.hashCode,
        c1 != c3
      )
    }
  )

  private def Try[A](a: => A): scala.util.Try[A] = scala.util.Try(a)
}
