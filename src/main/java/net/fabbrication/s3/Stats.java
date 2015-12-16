package net.fabbrication.s3;

/**
 * Struct for storing statistics.
 */
public class Stats {
  public double min;
  public double max;
  public double avg;
  public long count;
  public String description;

  private double total;

  public Stats(String description) {
    this.description = description;
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
  }

  public void accumulate(double val) {
    if (val < min)
      min = val;
    if (val > max)
      max = val;
    total += val;
    count++;
    avg = total / count;
  }

  @Override
  public String toString() {
    return description + "{" +
        "min=" + min +
        ", max=" + max +
        ", avg=" + avg +
        ", count=" + count +
        '}';
  }
}
