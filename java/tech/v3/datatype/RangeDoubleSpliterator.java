package tech.v3.datatype;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;


public class RangeDoubleSpliterator implements Spliterator.OfDouble {
  // Can never be greater that upTo, this avoids overflow if upper bound
  // is Long.MAX_VALUE
  // All elements are traversed if from == upTo & last == 0
  private long from;
  public final long upTo;
  // 1 if the range is closed and the last element has not been traversed
  // Otherwise, 0 if the range is open, or is a closed range and all
  // elements have been traversed
  private int last;
  public interface LongDoubleConverter
  {
    double longToDouble(long arg);
  };

  public final LongDoubleConverter converter;

  public RangeDoubleSpliterator(long from, long upTo, LongDoubleConverter converter,
				 boolean closed) {
    this(from, upTo, closed ? 1 : 0, converter);
  }

  private RangeDoubleSpliterator(long from, long upTo, int last,
				LongDoubleConverter converter) {
    Objects.requireNonNull(converter);
    assert upTo - from + last > 0;
    this.from = from;
    this.upTo = upTo;
    this.last = last;
    this.converter = converter;
  }

  @Override
  public boolean tryAdvance(DoubleConsumer consumer) {
    Objects.requireNonNull(consumer);

    final long i = from;
    if (i < upTo) {
      from++;
      consumer.accept(converter.longToDouble(i));
      return true;
    }
    else if (last > 0) {
      last = 0;
      consumer.accept(converter.longToDouble(i));
      return true;
    }
    return false;
  }

  @Override
  public void forEachRemaining(DoubleConsumer consumer) {
    Objects.requireNonNull(consumer);

    long i = from;
    final long hUpTo = upTo;
    int hLast = last;
    from = upTo;
    last = 0;
    while (i < hUpTo) {
      consumer.accept(converter.longToDouble(i++));
    }
    if (hLast > 0) {
      // Last element of closed range
      consumer.accept(converter.longToDouble(i));
    }
  }

  @Override
  public long estimateSize() {
    return upTo - from + last;
  }

  @Override
  public int characteristics() {
    return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED |
      Spliterator.IMMUTABLE | Spliterator.NONNULL;
  }

  @Override
  public Comparator<? super Double> getComparator() {
    return null;
  }

  @Override
  public Spliterator.OfDouble trySplit() {
    long size = estimateSize();
    return size <= 1
      ? null
      // Left split always has a half-open range
      : new RangeDoubleSpliterator(from, from = from + splitPoint(size), 0, converter);
  }

  /**
   * The spliterator size below which the spliterator will be split
   * at the mid-point to produce balanced splits. Above this size the
   * spliterator will be split at a ratio of
   * 1:(RIGHT_BALANCED_SPLIT_RATIO - 1)
   * to produce right-balanced splits.
   *
   * <p>Such splitting ensures that for very large ranges that the left
   * side of the range will more likely be processed at a lower-depth
   * than a balanced tree at the expense of a higher-depth for the right
   * side of the range.
   *
   * <p>This is optimized for cases such as LongStream.longs() that is
   * implemented as range of 0 to Long.MAX_VALUE but is likely to be
   * augmented with a limit operation that limits the number of elements
   * to a count lower than this threshold.
   */
  private static final long BALANCED_SPLIT_THRESHOLD = 1 << 24;

  /**
   * The split ratio of the left and right split when the spliterator
   * size is above BALANCED_SPLIT_THRESHOLD.
   */
  private static final long RIGHT_BALANCED_SPLIT_RATIO = 1 << 3;

  private long splitPoint(long size) {
    long d = (size < BALANCED_SPLIT_THRESHOLD) ? 2 : RIGHT_BALANCED_SPLIT_RATIO;
    // 2 <= size <= Long.MAX_VALUE
    return size / d;
  }
}
