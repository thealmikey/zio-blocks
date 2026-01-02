package zio.blocks.chunk

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuilder

sealed abstract class Chunk[+A] extends Serializable { self =>

  def length: Int

  def apply(index: Int): A

  def filter(f: A => Boolean): Chunk[A] = {
    val builder = ChunkBuilder.make[A]()
    foreach { a => if (f(a)) builder.addOne(a) }
    builder.result()
  }

  def map[B: ClassTag](f: A => B): Chunk[B] = {
    val builder = ChunkBuilder.make[B](length)
    foreach { a => builder.addOne(f(a)) }
    builder.result()
  }

  def takeWhile(f: A => Boolean): Chunk[A] = {
    val builder = ChunkBuilder.make[A]()
    val iter = chunkIterator
    var loop = true
    while (iter.hasNext && loop) {
      val a = iter.next()
      if (f(a)) builder.addOne(a) else loop = false
    }
    builder.result()
  }

  def dropWhile(f: A => Boolean): Chunk[A] = {
    val builder = ChunkBuilder.make[A]()
    val iter = chunkIterator
    var dropping = true
    while (iter.hasNext) {
      val a = iter.next()
      if (dropping && f(a)) ()
      else {
        dropping = false
        builder.addOne(a)
      }
    }
    builder.result()
  }

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
    override def foreach[U](f: Nothing => U): Unit = ()
    override def toArray[B >: Nothing : ClassTag]: Array[B] = Array.empty[B]
    override def filter(f: Nothing => Boolean): Chunk[Nothing] = this
    override def map[B: ClassTag](f: Nothing => B): Chunk[B] = this
    override def takeWhile(f: Nothing => Boolean): Chunk[Nothing] = this
    override def dropWhile(f: Nothing => Boolean): Chunk[Nothing] = this
    def chunkIterator: ChunkIterator[Nothing] = new ChunkIterator[Nothing] {
      def hasNext: Boolean = false
      def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
    }
  }

  private[chunk] final case class Singleton[A](value: A) extends Chunk[A] {
    def length: Int = 1
    def apply(index: Int): A = if (index == 0) value else throw new IndexOutOfBoundsException(index.toString)
    override def foreach[U](f: A => U): Unit = f(value)
    override def toArray[B >: A : ClassTag]: Array[B] = {
      val arr = new Array[B](1)
      arr(0) = value
      arr
    }
    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var consumed = false
      def hasNext: Boolean = !consumed
      def next(): A = if (!consumed) { consumed = true; value } else throw new NoSuchElementException()
    }
  }

  private[chunk] final class Arr[A](private val array: Array[AnyRef]) extends Chunk[A] {
    def length: Int = array.length
    def apply(index: Int): A = array(index).asInstanceOf[A]
    override def foreach[U](f: A => U): Unit = {
      var i = 0
      while (i < array.length) {
        f(array(i).asInstanceOf[A])
        i += 1
      }
    }
    override def filter(f: A => Boolean): Chunk[A] = {
      val builder = ChunkBuilder.make[A]()
      var i       = 0
      while (i < array.length) {
        val a = array(i).asInstanceOf[A]
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: A => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i).asInstanceOf[A]))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: A => Boolean): Chunk[A] = {
      val builder = ChunkBuilder.make[A]()
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i).asInstanceOf[A]
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: A => Boolean): Chunk[A] = {
      val builder  = ChunkBuilder.make[A]()
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i).asInstanceOf[A]
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: A: ClassTag]: Array[B] = {
      val target = new Array[B](length)
      val tag    = implicitly[ClassTag[B]]
      if (tag.runtimeClass.isPrimitive) {
        var i = 0
        while (i < length) {
          target(i) = array(i).asInstanceOf[B]
          i += 1
        }
      } else {
        System.arraycopy(array, 0, target, 0, length)
      }
      target
    }
    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): A = {
        val a = array(i).asInstanceOf[A]
        i += 1
        a
      }
    }
  }

  private[chunk] final class ByteArray(private val array: Array[Byte]) extends Chunk[Byte] {
    def length: Int = array.length
    def apply(index: Int): Byte = array(index)
    override def foreach[U](f: Byte => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Byte => Boolean): Chunk[Byte] = {
      val builder = new ByteChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Byte => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Byte => Boolean): Chunk[Byte] = {
      val builder = new ByteChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Byte => Boolean): Chunk[Byte] = {
      val builder  = new ByteChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Byte: ClassTag]: Array[B] = {
      if (Tags.isByte[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Byte] = new ChunkIterator[Byte] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Byte = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class CharArray(private val array: Array[Char]) extends Chunk[Char] {
    def length: Int = array.length
    def apply(index: Int): Char = array(index)
    override def foreach[U](f: Char => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Char => Boolean): Chunk[Char] = {
      val builder = new CharChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Char => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Char => Boolean): Chunk[Char] = {
      val builder = new CharChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Char => Boolean): Chunk[Char] = {
      val builder  = new CharChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Char: ClassTag]: Array[B] = {
      if (Tags.isChar[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Char] = new ChunkIterator[Char] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Char = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class ShortArray(private val array: Array[Short]) extends Chunk[Short] {
    def length: Int = array.length
    def apply(index: Int): Short = array(index)
    override def foreach[U](f: Short => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Short => Boolean): Chunk[Short] = {
      val builder = new ShortChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Short => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Short => Boolean): Chunk[Short] = {
      val builder = new ShortChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Short => Boolean): Chunk[Short] = {
      val builder  = new ShortChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Short: ClassTag]: Array[B] = {
      if (Tags.isShort[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Short] = new ChunkIterator[Short] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Short = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class IntArray(private val array: Array[Int]) extends Chunk[Int] {
    def length: Int = array.length
    def apply(index: Int): Int = array(index)
    override def foreach[U](f: Int => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Int => Boolean): Chunk[Int] = {
      val builder = new IntChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Int => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Int => Boolean): Chunk[Int] = {
      val builder = new IntChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Int => Boolean): Chunk[Int] = {
      val builder  = new IntChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Int: ClassTag]: Array[B] = {
      if (Tags.isInt[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Int] = new ChunkIterator[Int] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Int = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class LongArray(private val array: Array[Long]) extends Chunk[Long] {
    def length: Int = array.length
    def apply(index: Int): Long = array(index)
    override def foreach[U](f: Long => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Long => Boolean): Chunk[Long] = {
      val builder = new LongChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Long => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Long => Boolean): Chunk[Long] = {
      val builder = new LongChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Long => Boolean): Chunk[Long] = {
      val builder  = new LongChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Long: ClassTag]: Array[B] = {
      if (Tags.isLong[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Long] = new ChunkIterator[Long] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Long = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class FloatArray(private val array: Array[Float]) extends Chunk[Float] {
    def length: Int = array.length
    def apply(index: Int): Float = array(index)
    override def foreach[U](f: Float => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Float => Boolean): Chunk[Float] = {
      val builder = new FloatChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Float => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Float => Boolean): Chunk[Float] = {
      val builder = new FloatChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Float => Boolean): Chunk[Float] = {
      val builder  = new FloatChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Float: ClassTag]: Array[B] = {
      if (Tags.isFloat[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Float] = new ChunkIterator[Float] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Float = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class DoubleArray(private val array: Array[Double]) extends Chunk[Double] {
    def length: Int = array.length
    def apply(index: Int): Double = array(index)
    override def foreach[U](f: Double => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Double => Boolean): Chunk[Double] = {
      val builder = new DoubleChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Double => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Double => Boolean): Chunk[Double] = {
      val builder = new DoubleChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Double => Boolean): Chunk[Double] = {
      val builder  = new DoubleChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Double: ClassTag]: Array[B] = {
      if (Tags.isDouble[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Double] = new ChunkIterator[Double] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Double = { val a = array(i); i += 1; a }
    }
  }

  private[chunk] final class BooleanArray(private val array: Array[Boolean]) extends Chunk[Boolean] {
    def length: Int = array.length
    def apply(index: Int): Boolean = array(index)
    override def foreach[U](f: Boolean => U): Unit = {
      var i = 0
      while (i < array.length) { f(array(i)); i += 1 }
    }
    override def filter(f: Boolean => Boolean): Chunk[Boolean] = {
      val builder = new BooleanChunkBuilder(length)
      var i       = 0
      while (i < array.length) {
        val a = array(i)
        if (f(a)) builder.addOne(a)
        i += 1
      }
      builder.result()
    }
    override def map[B: ClassTag](f: Boolean => B): Chunk[B] = {
      val builder = ChunkBuilder.make[B](length)
      var i       = 0
      while (i < array.length) {
        builder.addOne(f(array(i)))
        i += 1
      }
      builder.result()
    }
    override def takeWhile(f: Boolean => Boolean): Chunk[Boolean] = {
      val builder = new BooleanChunkBuilder(length)
      var i       = 0
      var loop    = true
      while (i < array.length && loop) {
        val a = array(i)
        if (f(a)) builder.addOne(a) else loop = false
        i += 1
      }
      builder.result()
    }
    override def dropWhile(f: Boolean => Boolean): Chunk[Boolean] = {
      val builder  = new BooleanChunkBuilder(length)
      var i        = 0
      var dropping = true
      while (i < array.length) {
        val a = array(i)
        if (dropping && f(a)) ()
        else {
          dropping = false
          builder.addOne(a)
        }
        i += 1
      }
      builder.result()
    }
    override def toArray[B >: Boolean: ClassTag]: Array[B] = {
      if (Tags.isBoolean[B]) array.asInstanceOf[Array[B]]
      else {
        val target = new Array[B](length)
        var i      = 0
        while (i < length) { target(i) = array(i).asInstanceOf[B]; i += 1 }
        target
      }
    }
    def chunkIterator: ChunkIterator[Boolean] = new ChunkIterator[Boolean] {
      private[this] var i = 0
      def hasNext: Boolean = i < array.length
      def next(): Boolean = { val a = array(i); i += 1; a }
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
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.Arr[A](arr.asInstanceOf[Array[AnyRef]])
    }
  }

  private final class ByteChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Byte] {
    private[this] val builder = ArrayBuilder.make[Byte]
    def addOne(a: Byte): this.type = { builder += a; this }
    def result(): Chunk[Byte] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.ByteArray(arr)
    }
  }

  private final class ShortChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Short] {
    private[this] val builder = ArrayBuilder.make[Short]
    def addOne(a: Short): this.type = { builder += a; this }
    def result(): Chunk[Short] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.ShortArray(arr)
    }
  }

  private final class IntChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Int] {
    private[this] val builder = ArrayBuilder.make[Int]
    def addOne(a: Int): this.type = { builder += a; this }
    def result(): Chunk[Int] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.IntArray(arr)
    }
  }

  private final class LongChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Long] {
    private[this] val builder = ArrayBuilder.make[Long]
    def addOne(a: Long): this.type = { builder += a; this }
    def result(): Chunk[Long] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.LongArray(arr)
    }
  }

  private final class FloatChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Float] {
    private[this] val builder = ArrayBuilder.make[Float]
    def addOne(a: Float): this.type = { builder += a; this }
    def result(): Chunk[Float] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.FloatArray(arr)
    }
  }

  private final class DoubleChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Double] {
    private[this] val builder = ArrayBuilder.make[Double]
    def addOne(a: Double): this.type = { builder += a; this }
    def result(): Chunk[Double] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.DoubleArray(arr)
    }
  }

  private final class BooleanChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Boolean] {
    private[this] val builder = ArrayBuilder.make[Boolean]
    def addOne(a: Boolean): this.type = { builder += a; this }
    def result(): Chunk[Boolean] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.BooleanArray(arr)
    }
  }

  private final class CharChunkBuilder(initialCapacity: Int) extends ChunkBuilder[Char] {
    private[this] val builder = ArrayBuilder.make[Char]
    def addOne(a: Char): this.type = { builder += a; this }
    def result(): Chunk[Char] = {
      val arr = builder.result()
      if (arr.length == 0) Chunk.empty
      else if (arr.length == 1) Chunk.Singleton(arr(0))
      else new Chunk.CharArray(arr)
    }
  }
}
