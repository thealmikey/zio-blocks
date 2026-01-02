package zio.blocks.chunk

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuilder
import java.util.concurrent.atomic.AtomicInteger

sealed abstract class Chunk[+A] extends Serializable { self =>

  def length: Int

  def depth: Int = 0

  def apply(index: Int): A

  final def ++[A1 >: A](that: Chunk[A1]): Chunk[A1] = {
    if (self.isEmpty) that
    else if (that.isEmpty) self
    else {
      val newDepth = Math.max(self.depth, that.depth) + 1
      if (newDepth > Chunk.MaxDepthBeforeMaterialize) {
        Chunk.fromArray(self.toArray[AnyRef](ClassTag.AnyRef)).concat(that)
      } else {
        self.concat(that)
      }
    }
  }

  protected def concat[A1 >: A](that: Chunk[A1]): Chunk[A1] =
    Chunk.Concat(self, that)

  final def slice(from: Int, until: Int): Chunk[A] = {
    val low  = Math.max(from, 0)
    val high = Math.min(until, length)
    val len  = Math.max(0, high - low)
    if (len == 0) Chunk.empty
    else if (low == 0 && high == length) self
    else Chunk.Slice(self, low, len)
  }

  def filter(f: A => Boolean): Chunk[A] = {
    val builder = ChunkBuilder.make[A]()
    foreach { a => if (f(a)) builder.addOne(a) }
    builder.result()
  }

  def map[B: ClassTag](f: A => B): Chunk[B] = {
    val builder = ChunkBuilder.make[B](length)
    val iter    = chunkIterator
    while (iter.hasNext) {
      builder.addOne(f(iter.next()))
    }
    builder.result()
  }

  def flatMap[B: ClassTag](f: A => Chunk[B]): Chunk[B] = {
    val builder = ChunkBuilder.make[B]()
    val iter    = chunkIterator
    while (iter.hasNext) {
      f(iter.next()).foreach(builder.addOne)
    }
    builder.result()
  }

  def collect[B: ClassTag](pf: PartialFunction[A, B]): Chunk[B] = {
    val builder = ChunkBuilder.make[B]()
    val iter    = chunkIterator
    while (iter.hasNext) {
      val a = iter.next()
      if (pf.isDefinedAt(a)) builder.addOne(pf(a))
    }
    builder.result()
  }

  def collectWhile[B: ClassTag](pf: PartialFunction[A, B]): Chunk[B] = {
    val builder = ChunkBuilder.make[B]()
    val iter    = chunkIterator
    var loop    = true
    while (iter.hasNext && loop) {
      val a = iter.next()
      if (pf.isDefinedAt(a)) builder.addOne(pf(a))
      else loop = false
    }
    builder.result()
  }

  def take(n: Int): Chunk[A] = slice(0, n)

  def drop(n: Int): Chunk[A] = slice(n, length)

  def takeWhile(f: A => Boolean): Chunk[A] = {
    val builder = ChunkBuilder.make[A]()
    val iter    = chunkIterator
    var loop    = true
    while (iter.hasNext && loop) {
      val a = iter.next()
      if (f(a)) builder.addOne(a) else loop = false
    }
    builder.result()
  }

  def dropWhile(f: A => Boolean): Chunk[A] = {
    val iter     = chunkIterator
    var dropping = true
    var count    = 0
    while (iter.hasNext && dropping) {
      val a = iter.next()
      if (f(a)) count += 1
      else dropping = false
    }
    drop(count)
  }

  def splitAt(n: Int): (Chunk[A], Chunk[A]) = (take(n), drop(n))

  def partition(f: A => Boolean): (Chunk[A], Chunk[A]) = {
    val left  = ChunkBuilder.make[A]()
    val right = ChunkBuilder.make[A]()
    val iter  = chunkIterator
    while (iter.hasNext) {
      val a = iter.next()
      if (f(a)) left.addOne(a) else right.addOne(a)
    }
    (left.result(), right.result())
  }

  def partitionMap[B: ClassTag, C: ClassTag](f: A => Either[B, C]): (Chunk[B], Chunk[C]) = {
    val left  = ChunkBuilder.make[B]()
    val right = ChunkBuilder.make[C]()
    val iter  = chunkIterator
    while (iter.hasNext) {
      f(iter.next()) match {
        case Left(b)  => left.addOne(b)
        case Right(c) => right.addOne(c)
      }
    }
    (left.result(), right.result())
  }

  def span(f: A => Boolean): (Chunk[A], Chunk[A]) = {
    val iter  = chunkIterator
    var count = 0
    var loop  = true
    while (iter.hasNext && loop) {
      if (f(iter.next())) count += 1
      else loop = false
    }
    splitAt(count)
  }

  def foldLeft[S](s: S)(f: (S, A) => S): S = {
    var res  = s
    val iter = chunkIterator
    while (iter.hasNext) {
      res = f(res, iter.next())
    }
    res
  }

  def foldRight[S](s: S)(f: (A, S) => S): S = {
    var res = s
    var i   = length - 1
    while (i >= 0) {
      res = f(self(i), res)
      i -= 1
    }
    res
  }

  def foldWhile[S](s: S)(p: S => Boolean)(f: (S, A) => S): S = {
    var res  = s
    val iter = chunkIterator
    while (iter.hasNext && p(res)) {
      res = f(res, iter.next())
    }
    res
  }

  def mapAccum[S, B: ClassTag](s: S)(f: (S, A) => (S, B)): (S, Chunk[B]) = {
    val builder = ChunkBuilder.make[B](length)
    var res     = s
    val iter    = chunkIterator
    while (iter.hasNext) {
      val tuple = f(res, iter.next())
      res = tuple._1
      builder.addOne(tuple._2)
    }
    (res, builder.result())
  }

  def zip[B](that: Chunk[B]): Chunk[(A, B)] =
    zipWith(that)((_, _))

  def zipWith[B, C: ClassTag](that: Chunk[B])(f: (A, B) => C): Chunk[C] = {
    val len     = Math.min(self.length, that.length)
    val builder = ChunkBuilder.make[C](len)
    var i       = 0
    while (i < len) {
      builder.addOne(f(self(i), that(i)))
      i += 1
    }
    builder.result()
  }

  def zipAll[B](that: Chunk[B], selfDefault: A, thatDefault: B): Chunk[(A, B)] = {
    val len     = Math.max(self.length, that.length)
    val builder = ChunkBuilder.make[(A, B)](len)
    var i       = 0
    while (i < len) {
      val a = if (i < self.length) self(i) else selfDefault
      val b = if (i < that.length) that(i) else thatDefault
      builder.addOne((a, b))
      i += 1
    }
    builder.result()
  }

  def zipWithIndex: Chunk[(A, Int)] = zipWithIndexFrom(0)

  def zipWithIndexFrom(n: Int): Chunk[(A, Int)] = {
    val builder = ChunkBuilder.make[(A, Int)](length)
    val iter    = chunkIterator
    var i       = n
    while (iter.hasNext) {
      builder.addOne((iter.next(), i))
      i += 1
    }
    builder.result()
  }

  def grouped(n: Int): Chunk[Chunk[A]] = {
    if (n <= 0) throw new IllegalArgumentException("n must be positive")
    val builder = ChunkBuilder.make[Chunk[A]]()
    var i       = 0
    while (i < length) {
      builder.addOne(slice(i, i + n))
      i += n
    }
    builder.result()
  }

  def sliding(size: Int, step: Int = 1): Chunk[Chunk[A]] = {
    if (size <= 0 || step <= 0) throw new IllegalArgumentException("size and step must be positive")
    val builder = ChunkBuilder.make[Chunk[A]]()
    var i       = 0
    while (i < length) {
      builder.addOne(slice(i, i + size))
      if (i + size >= length) i = length
      else i += step
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

  def fromArray[A: ClassTag](array: Array[A]): Chunk[A] = {
    if (array.length == 0) empty
    else if (array.length == 1) Singleton(array(0))
    else {
      val tag = implicitly[ClassTag[A]]
      if (Tags.isByte(tag)) new ByteArray(array.asInstanceOf[Array[Byte]]).asInstanceOf[Chunk[A]]
      else if (Tags.isChar(tag)) new CharArray(array.asInstanceOf[Array[Char]]).asInstanceOf[Chunk[A]]
      else if (Tags.isShort(tag)) new ShortArray(array.asInstanceOf[Array[Short]]).asInstanceOf[Chunk[A]]
      else if (Tags.isInt(tag)) new IntArray(array.asInstanceOf[Array[Int]]).asInstanceOf[Chunk[A]]
      else if (Tags.isLong(tag)) new LongArray(array.asInstanceOf[Array[Long]]).asInstanceOf[Chunk[A]]
      else if (Tags.isFloat(tag)) new FloatArray(array.asInstanceOf[Array[Float]]).asInstanceOf[Chunk[A]]
      else if (Tags.isDouble(tag)) new DoubleArray(array.asInstanceOf[Array[Double]]).asInstanceOf[Chunk[A]]
      else if (Tags.isBoolean(tag)) new BooleanArray(array.asInstanceOf[Array[Boolean]]).asInstanceOf[Chunk[A]]
      else new Arr[A](array.asInstanceOf[Array[AnyRef]])
    }
  }

  private[chunk] final case class Concat[A](left: Chunk[A], right: Chunk[A]) extends Chunk[A] {
    val length: Int = left.length + right.length
    override val depth: Int = Math.max(left.depth, right.depth) + 1

    def apply(index: Int): A =
      if (index < left.length) left(index) else right(index - left.length)

    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var currIter = left.chunkIterator
      private[this] var onLeft   = true
      def hasNext: Boolean = currIter.hasNext || (onLeft && right.isNotEmpty)
      def next(): A = {
        if (!currIter.hasNext && onLeft) {
          onLeft = false
          currIter = right.chunkIterator
        }
        currIter.next()
      }
    }
  }

  private[chunk] final case class Slice[A](chunk: Chunk[A], offset: Int, length: Int) extends Chunk[A] {
    override def depth: Int = chunk.depth

    def apply(index: Int): A = {
      if (index < 0 || index >= length) throw new IndexOutOfBoundsException(index.toString)
      chunk(index + offset)
    }

    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < length
      def next(): A = {
        if (!hasNext) throw new NoSuchElementException()
        val a = chunk(i + offset)
        i += 1
        a
      }
    }

    override def toArray[B >: A: ClassTag]: Array[B] = {
      val target = new Array[B](length)
      val tag    = implicitly[ClassTag[B]]
      if (Tags.isAnyRef(tag)) {
        chunk match {
          case arr: Arr[_] =>
            System.arraycopy(arr.array, offset, target, 0, length)
          case b: ByteArray if Tags.isByte(tag) =>
            System.arraycopy(b.array, offset, target, 0, length)
          case _ =>
            copyToArray(target)
        }
      } else {
        copyToArray(target)
      }
      target
    }

    private def copyToArray[B >: A](target: Array[B]): Unit = {
      var i = 0
      while (i < length) {
        target(i) = chunk(i + offset)
        i += 1
      }
    }
  }

  private[chunk] final class AppendN[A](
    private[chunk] val buffer: Array[AnyRef],
    private[chunk] val refCount: AtomicInteger,
    val length: Int
  ) extends Chunk[A] {
    def apply(index: Int): A = {
      if (index < 0 || index >= length) throw new IndexOutOfBoundsException(index.toString)
      buffer(index).asInstanceOf[A]
    }

    override protected def concat[A1 >: A](that: Chunk[A1]): Chunk[A1] = {
      if (length < buffer.length && refCount.get() == 1) {
        that match {
          case Singleton(v) =>
            buffer(length) = v.asInstanceOf[AnyRef]
            new AppendN(buffer, refCount, length + 1)
          case _ => super.concat(that)
        }
      } else super.concat(that)
    }

    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < length
      def next(): A = {
        if (!hasNext) throw new NoSuchElementException()
        val a = buffer(i).asInstanceOf[A]
        i += 1
        a
      }
    }

    override def toArray[B >: A: ClassTag]: Array[B] = {
      val target = new Array[B](length)
      System.arraycopy(buffer, 0, target, 0, length)
      target
    }
  }

  private[chunk] final class PrependN[A](
    private[chunk] val buffer: Array[AnyRef],
    private[chunk] val refCount: AtomicInteger,
    private[chunk] val startIndex: Int,
    val length: Int
  ) extends Chunk[A] {
    def apply(index: Int): A = {
      if (index < 0 || index >= length) throw new IndexOutOfBoundsException(index.toString)
      buffer(startIndex + index).asInstanceOf[A]
    }

    override protected def concat[A1 >: A](that: Chunk[A1]): Chunk[A1] = {
      val selfChunk = this.asInstanceOf[Chunk[A1]]
      if (startIndex > 0 && refCount.get() == 1) {
        selfChunk match {
          case _ if that.length == 1 =>
            val v = that(0)
            val newStart = startIndex - 1
            buffer(newStart) = v.asInstanceOf[AnyRef]
            new PrependN(buffer, refCount, newStart, length + 1).asInstanceOf[Chunk[A1]]
          case _ => super.concat(that)
        }
      } else super.concat(that)
    }

    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < length
      def next(): A = {
        if (!hasNext) throw new NoSuchElementException()
        val a = buffer(startIndex + i).asInstanceOf[A]
        i += 1
        a
      }
    }

    override def toArray[B >: A: ClassTag]: Array[B] = {
      val target = new Array[B](length)
      System.arraycopy(buffer, startIndex, target, 0, length)
      target
    }
  }

  private[chunk] final case class Update[A](chunk: Chunk[A], index: Int, value: A) extends Chunk[A] {
    override def depth: Int = chunk.depth + 1
    def length: Int = chunk.length
    def apply(i: Int): A = {
      if (i < 0 || i >= length) throw new IndexOutOfBoundsException(i.toString)
      if (i == index) value else chunk(i)
    }
    def chunkIterator: ChunkIterator[A] = new ChunkIterator[A] {
      private[this] var i = 0
      def hasNext: Boolean = i < length
      def next(): A = {
        if (!hasNext) throw new NoSuchElementException()
        val a = if (i == index) value else chunk(i)
        i += 1
        a
      }
    }
  }

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
      var i = 0
      while (i < array.length && f(array(i).asInstanceOf[A])) {
        i += 1
      }
      take(i)
    }
    override def dropWhile(f: A => Boolean): Chunk[A] = {
      var i = 0
      while (i < array.length && f(array(i).asInstanceOf[A])) {
        i += 1
      }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, A) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) {
        res = f(res, array(i).asInstanceOf[A])
        i += 1
      }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) {
        i += 1
      }
      take(i)
    }
    override def dropWhile(f: Byte => Boolean): Chunk[Byte] = {
      var i = 0
      while (i < array.length && f(array(i))) {
        i += 1
      }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Byte) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) {
        res = f(res, array(i))
        i += 1
      }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Char => Boolean): Chunk[Char] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Char) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }
    override def foldRight[S](s: S)(f: (Char, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Short => Boolean): Chunk[Short] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Short) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Short, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Int => Boolean): Chunk[Int] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Int) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Int, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Long => Boolean): Chunk[Long] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Long) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Long, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Float => Boolean): Chunk[Float] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Float) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Float, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Double => Boolean): Chunk[Double] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Double) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Double, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      take(i)
    }
    override def dropWhile(f: Boolean => Boolean): Chunk[Boolean] = {
      var i = 0
      while (i < array.length && f(array(i))) { i += 1 }
      drop(i)
    }
    override def foldLeft[S](s: S)(f: (S, Boolean) => S): S = {
      var res = s
      var i   = 0
      while (i < array.length) { res = f(res, array(i)); i += 1 }
      res
    }

    override def foldRight[S](s: S)(f: (Boolean, S) => S): S = {
      var res = s
      var i   = array.length - 1
      while (i >= 0) { res = f(array(i), res); i -= 1 }
      res
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
    def isAnyRef[A](implicit tag: ClassTag[A]): Boolean  = !tag.runtimeClass.isPrimitive
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
