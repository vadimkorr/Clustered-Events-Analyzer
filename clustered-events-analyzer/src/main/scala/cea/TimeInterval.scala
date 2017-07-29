package cea

case class TimeInterval(
    private var _start: Long, 
    private var _end: Long) {  
  require(_start <= _end, "Start must be less than or equal to End")
  
  def <(timeInterval: TimeInterval): Boolean = this.length < timeInterval.length
  def <=(timeInterval: TimeInterval): Boolean = this.length <= timeInterval.length

  def >(timeInterval: TimeInterval): Boolean = this.length > timeInterval.length
  def >=(timeInterval: TimeInterval): Boolean = this.length >= timeInterval.length

  def length: Long = _end - _start

  def start = _start
  def end = _end

  def setStart(start: Long): TimeInterval = {
    _start = start
    this
  }

  def setEnd(end: Long): TimeInterval = {
    _end = end
    this
  }

  override def toString = s"${_start.toString};${_end.toString}"
}

object TimeInterval {
  def apply(start: Long): TimeInterval = { 
    TimeInterval(start, start)
  }
}
