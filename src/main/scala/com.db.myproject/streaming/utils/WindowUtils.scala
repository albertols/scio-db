package com.db.myproject.streaming.utils

import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.Duration

object WindowUtils {

  /**
   * Group messages in windows using Beam.
   *
   * @param ps
   * SCollection of messages to be grouped in windows
   * @return
   * SCollection of tuples, with the window and an iterable over the messages
   */
  def toIntervalWindow[T](windowSize: Long, ps: SCollection[T]): SCollection[(IntervalWindow, Iterable[T])] =
    ps
      .withFixedWindows(org.joda.time.Duration.standardSeconds(windowSize))
      .withWindow[IntervalWindow]
      .swap
      .groupByKey

  def toIntervalWindowAndIterable[T](windowSize: Long, messages: SCollection[T]): SCollection[(IntervalWindow, Iterable[T])] = {
    val groups: SCollection[(IntervalWindow, Iterable[T])] =
      messages.transform("Windowing BTRSink")(WindowUtils.toIntervalWindow(windowSize, _))
    groups
  }

  /**
   *
   * @param prefixName, param for withName
   * @param windowType, fixed ot global
   * @param in, the SCollection to be windowed
   * @param duration, only for fixed
   * @param winOptions
   * @tparam T, generic class type
   * @return the Windowed SCollection[T]
   */
  def applyFixedOrGlobalToSCollection[T](
                                   prefixName: String,
                                   windowType: String,
                                   in: SCollection[T],
                                   duration: org.joda.time.Duration = Duration.standardSeconds(10),
                                   winOptions: Option[WindowOptions] = None
                                 ): SCollection[T] =
    if ("fixed".equals(windowType)) winOptions match {
      case Some(opt) => in.withFixedWindows(duration, options = opt).withName(s"$prefixName Fixed Window(options)")
      case None => in.withFixedWindows(duration).withName(s"$prefixName Fixed Window")
    }
    else
      winOptions match {
        case Some(opt) => in.withGlobalWindow(options = opt).withName(s"$prefixName Global Window(options)")
        case None => in
      }
}
