module GtfsDf
  module Utils
    SECONDS_IN_MINUTE = 60
    SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60
    SECONDS_IN_DAY = SECONDS_IN_HOUR * 24

    # Converts a GTFS time string column to a seconds since midnight integer
    #
    # The input string is expected to be in the HH:MM:SS format (H:MM:SS is
    # also accepted).
    #
    # @example dataframe.with_columns(GtfsDf::Utils.as_seconds_since_midnight('start_time'))
    # This will return the dataframe with the start_time column as seconds since midnight
    #
    # @param col_name String The column to convert
    def self.as_seconds_since_midnight(col_name)
      parts = Polars.col(col_name).str.split(":")

      hours = parts.list.get(0).cast(:i64)
      minutes = parts.list.get(1).cast(:i64)
      seconds = parts.list.get(2).cast(:i64)

      (hours * SECONDS_IN_HOUR) +
        (minutes * SECONDS_IN_MINUTE) +
        seconds
    end

    # Converts a GTFS seconds since midnight integer to a time string with the format HH:MM:SS
    #
    # @example dataframe.with_columns(GtfsDf::Utils.as_time_string('start_time'))
    # This will return the dataframe with the start_time column as an HH:MM:SS string
    #
    # @param col_name String the column to convert
    def self.as_time_string(col_name)
      total_seconds = Polars.col(col_name)
      hours = total_seconds.floordiv(SECONDS_IN_HOUR)
      minutes = (total_seconds % SECONDS_IN_HOUR).floordiv(SECONDS_IN_MINUTE)
      seconds = (total_seconds % SECONDS_IN_MINUTE)

      Polars.format(
        "{}:{}:{}",
        hours.cast(:str).str.zfill(2),
        minutes.cast(:str).str.zfill(2),
        seconds.cast(:str).str.zfill(2)
      )
    end

    # Parses a GTFS time string
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
    # @param str String
    # Parses a GTFS time string or returns integer seconds if already provided.
    # Accepts Integer (returns as-is), or HH:MM:SS string (possibly >24h).
    def self.parse_time(str)
      return str if str.is_a?(Integer)
      return nil if str.nil? || (str.respond_to?(:strip) && str.strip.empty?)

      parts = str.to_s.split(":")
      return nil unless parts.size == 3 && parts.all? { |p| p.match?(/^\d+$/) }

      hours, mins, secs = parts.map(&:to_i)
      hours * 3600 + mins * 60 + secs
    rescue
      nil
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
    def self.parse_date(str)
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
