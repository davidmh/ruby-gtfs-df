module GtfsDf
  module Utils
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

      parts = str.to_s.split(':')
      return nil unless parts.size == 3 && parts.all? { |p| p.match?(/^\d+$/) }

      hours, mins, secs = parts.map(&:to_i)
      hours * 3600 + mins * 60 + secs
    rescue StandardError
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
        Date.strptime(str, '%Y%m%d')
      rescue ArgumentError
        nil
      end
    end
  end
end
