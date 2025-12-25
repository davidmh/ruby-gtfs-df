module GtfsDf
  module Utils
    extend self

    SECONDS_IN_MINUTE = 60
    SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60
    SECONDS_IN_DAY = SECONDS_IN_HOUR * 24

    # Parses a GTFS time string to seconds since midnight
    #
    # The input string is expected to be in the HH:MM:SS format (H:MM:SS is
    # also accepted).
    #
    # The time is measured from "noon minus 12h" of the service day
    # (effectively midnight except for days on which daylight savings time
    # changes occur). For times occurring after midnight on the service day,
    # enter the time as a value greater than 24:00:00 in HH:MM:SS.
    #
    # @example 14:30:00 for 2:30PM or
    # 25:35:00 for 1:35AM on the next day.
    #
    # @param str String|Integer
    # @return Integer|nil seconds since midnight, or nil if invalid
    def parse_time(str)
      return str if str.is_a?(Integer)
      return nil if str.nil? || (str.respond_to?(:strip) && str.strip.empty?)

      parts = str.to_s.split(":")
      return nil unless parts.size == 3 && parts.all? { |p| p.match?(/^\d+$/) }

      hours, mins, secs = parts.map(&:to_i)
      hours * 3600 + mins * 60 + secs
    rescue
      nil
    end

    # Formats seconds since midnight as a GTFS time string (HH:MM:SS)
    #
    # Handles times greater than 24 hours for times that span past midnight.
    #
    # @param seconds Integer seconds since midnight
    # @return String|nil time in HH:MM:SS format, or nil if invalid
    def format_time(seconds)
      return nil if seconds.nil?
      return seconds if seconds.is_a?(String)

      hours = seconds / SECONDS_IN_HOUR
      minutes = (seconds % SECONDS_IN_HOUR) / SECONDS_IN_MINUTE
      secs = seconds % SECONDS_IN_MINUTE

      format("%02d:%02d:%02d", hours, minutes, secs)
    rescue
      nil
    end

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

      hours = parts.list.get(0).cast(:i64)
      minutes = parts.list.get(1).cast(:i64)
      seconds = parts.list.get(2).cast(:i64)

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
        hours.cast(:str).str.zfill(2),
        minutes.cast(:str).str.zfill(2),
        seconds.cast(:str).str.zfill(2)
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
    # @param str String
    def parse_date(str)
      return nil if str.nil? || str.strip.empty?
      return nil unless str.match?(/^\d{8}$/)

      begin
        Date.strptime(str, "%Y%m%d")
      rescue ArgumentError
        nil
      end
    end
  end
end
