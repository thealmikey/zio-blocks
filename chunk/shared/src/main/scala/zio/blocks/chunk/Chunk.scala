package zio.blocks.chunk

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuilder

sealed abstract class Chunk[+A] extends Serializable { self =>

  def length: Int

  def apply(index: Int): A

  final def isEmpty: Boolean = length == 0

  final def isNotEmpty: Boolean = !isEmpty

  def head: A = apply(0)

  def headOption: Option[A] = if (isEmpty) None else Some(apply(0))

  def lastOption: Option[A] = if (isEmpty) None else Some(apply(length - 1))

  def chunkIterator: ChunkIterator[A]

  final def iterator: Iterator[A] = new Iterator[A] {
    private[this] val iter = self.chunkIterator
    def hasNext: Boolean = iter.hasNext
    def next(): A = iter.next()
  }

  def foreach[U](f: A => U): Unit = {
    val iter = chunkIterator
    while (iter.hasNext) {
      f(iter.next())
    }
  }

  def toArray[B >: A: ClassTag]: Array[B] = {
    val array = new Array[B](length)
    val iter = chunkIterator
    var i = 0
    while (iter.hasNext) {
      array(i) = iter.next()
      i += 1
    }
    array
  }
}

object Chunk {
  private[chunk] val BufferSize = 64
  private[chunk] val MaxDepthBeforeMaterialize = 128
  private[chunk] val UpdateBufferSize = 256

  def empty[A]: Chunk[A] = Empty

  private[chunk] case object Empty extends Chunk[Nothing] {
    def length: Int = 0
    def apply(index: Int): Nothing = throw new IndexOutOfBoundsException(index.toString)
    def chunkIterator: ChunkIterator[Nothing] = new ChunkIterator[Nothing] {
      def hasNext: Boolean = false
      def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
    }
  }

  private[chunk] object Tags {
    def isByte[A](implicit tag: ClassTag[A]): Boolean    = tag == ClassTag.Byte
    def isShort[A](implicit tag: ClassTag[A]): Boolean   = tag == ClassTag.Short
    def isInt[A](implicit tag: ClassTag[A]): Boolean     = tag == ClassTag.Int
    def isLong[A](implicit tag: ClassTag[A]): Boolean    = tag == ClassTag.Long
    def isFloat[A](implicit tag: ClassTag[A]): Boolean   = tag == ClassTag.Float
    def isDouble[A](implicit tag: ClassTag[A]): Boolean  = tag == ClassTag.Double
    def isBoolean[A](implicit tag: ClassTag[A]): Boolean = tag == ClassTag.Boolean
    def isChar[A](implicit tag: ClassTag[A]): Boolean    = tag == ClassTag.Char
  }
}

sealed trait ChunkIterator[+A] {
  def hasNext: Boolean
  def next(): A
}

trait ChunkBuilder[A] {
  def addOne(a: A): this.type
  def result(): Chunk[A]
}

object ChunkBuilder {
  def make[A: ClassTag](initialCapacity: Int = Chunk.BufferSize): ChunkBuilder[A] = {
    val tag = implicitly[ClassTag[A]]
    val builder = 
      if (Chunk.Tags.isByte(tag)) new ByteChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isShort(tag)) new ShortChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isInt(tag)) new IntChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isLong(tag)) new LongChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isFloat(tag)) new FloatChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isDouble(tag)) new DoubleChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isBoolean(tag)) new BooleanChunkBuilder(initialCapacity)
      else if (Chunk.Tags.isChar(tag)) new CharChunkBuilder(initialCapacity)
      else new GenericChunkBuilder[A](initialCapacity)
    
    builder.asInstanceOf[ChunkBuilder[A]]
  }

  private final class GenericChunkBuilder[A: ClassTag](initialCapacity: Int) extends ChunkBuilder[A] {
    private[this] val builder = ArrayBuilder.make[A]
    def addOne(a: A): this.type = { builder += a; this }
    def result(): Chunk[A] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class ByteChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Byte] {
    private[this] val builder = ArrayBuilder.make[Byte]
    def addOne(a: Byte): this.type = { builder += a; this }
    def result(): Chunk[Byte] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class ShortChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Short] {
    private[this] val builder = ArrayBuilder.make[Short]
    def addOne(a: Short): this.type = { builder += a; this }
    def result(): Chunk[Short] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class IntChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Int] {
    private[this] val builder = ArrayBuilder.make[Int]
    def addOne(a: Int): this.type = { builder += a; this }
    def result(): Chunk[Int] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class LongChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Long] {
    private[this] val builder = ArrayBuilder.make[Long]
    def addOne(a: Long): this.type = { builder += a; this }
    def result(): Chunk[Long] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class FloatChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Float] {
    private[this] val builder = ArrayBuilder.make[Float]
    def addOne(a: Float): this.type = { builder += a; this }
    def result(): Chunk[Float] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class DoubleChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Double] {
    private[this] val builder = ArrayBuilder.make[Double]
    def addOne(a: Double): this.type = { builder += a; this }
    def result(): Chunk[Double] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class BooleanChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Boolean] {
    private[this] val builder = ArrayBuilder.make[Boolean]
    def addOne(a: Boolean): this.type = { builder += a; this }
    def result(): Chunk[Boolean] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class CharChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Char] {
    private[this] val builder = ArrayBuilder.make[Char]
    def addOne(a: Char): this.type = { builder += a; this }
    def result(): Chunk[Char] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty else new ArrayChunk(arr)
    }
  }

  private final class ArrayChunk[A](private val array: Array[A]) extends Chunk[A] {
    def length: Int = array.length
    def apply(index: Int): A = array(index)
    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): A = {
        val a = array(i)
        i += 1
        a
      }
    }
  }
}
