module GtfsDf
  module Utils
    extend self

    SECONDS_IN_MINUTE = 60
    SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60
    SECONDS_IN_DAY = SECONDS_IN_HOUR * 24

    # Converts a GTFS time string column to seconds since midnight
    #
    # Use this method with Polars DataFrames to convert time columns.
    #
    # @example dataframe.with_columns(GtfsDf::Utils.as_seconds_since_midnight('start_time'))
    #
    # @param col_name String The column to convert
    # @return Polars::Expr
    def as_seconds_since_midnight(col_name)
      parts = Polars.col(col_name).str.split(":")

      hours = parts.list.get(0).cast(Polars::Int64)
      minutes = parts.list.get(1).cast(Polars::Int64)
      seconds = parts.list.get(2).cast(Polars::Int64)

      (hours * SECONDS_IN_HOUR) +
        (minutes * SECONDS_IN_MINUTE) +
        seconds
    end

    # Converts a seconds since midnight column to GTFS time string (HH:MM:SS)
    #
    # Use this method with Polars DataFrames to convert time columns back to strings.
    #
    # @example dataframe.with_columns(GtfsDf::Utils.as_time_string('start_time'))
    #
    # @param col_name String The column to convert
    # @return Polars::Expr
    def as_time_string(col_name)
      total_seconds = Polars.col(col_name)
      hours = total_seconds.floordiv(SECONDS_IN_HOUR)
      minutes = (total_seconds % SECONDS_IN_HOUR).floordiv(SECONDS_IN_MINUTE)
      seconds = total_seconds % SECONDS_IN_MINUTE

      Polars.format(
        "{}:{}:{}",
        hours.cast(Polars::String).str.zfill(2),
        minutes.cast(Polars::String).str.zfill(2),
        seconds.cast(Polars::String).str.zfill(2)
      )
    end

    # Converts a seconds since midnight Series to GTFS time strings for inspection
    #
    # Use this method to get a readable view of time columns during debugging.
    # It's not meant to be performant.
    #
    # @example GtfsDf::Utils.inspect_time(feed.stop_times["arrival_time"])
    #
    # @param series Polars::Series The series to convert
    # @return Polars::Series A series with time strings
    def inspect_time(series)
      series.to_frame.with_columns(
        as_time_string(series.name)
      )[series.name]
    end

    # Parses a GTFS date string
    #
    # The input string is expected to be a service day in the YYYYMMDD format.
    # Since time within a service day may be above 24:00:00, a service day may
    # contain information for the subsequent day(s).
    #
    # @example 20180913 for September 13th, 2018.
    #
    # @param col Polars::Expr
    def parse_date(col)
      col.str.strptime(Polars::Date, "%Y%m%d", strict: false)
    end
  end
end
