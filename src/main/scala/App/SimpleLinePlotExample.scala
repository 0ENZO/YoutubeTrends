package App

import xyz.devfortress.splot.ColorLike.colorColorLike
import xyz.devfortress.splot.LinesTypeLike.lineTypeLineTypeLike

import java.lang.Math.{pow, sin}
import xyz.devfortress.splot._

object SimpleLinePlotExample {
  def f(x: Double): Double = sin(pow(x, 2)) / x

  def main(args: Array[String]): Unit = {
    val fig = Figure(
      title = "sin(x^2)/x",
    )
    val xs = 1.0 to 5.0 by 0.001

    fig.plot(xs.map(x => (x, 1/x)), color = "blue", lw = 2, lt = "--")
    fig.plot(xs.map(x => (x, f(x))))
    fig.show(730, 500)
  }
}