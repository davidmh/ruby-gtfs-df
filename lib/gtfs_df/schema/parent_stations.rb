# frozen_string_literal: true

module GtfsDf
  module Schema
    # NOTE: In GTFS, this is represented by the stops.txt file. We split it out explicitly
    # so that we don't need to handle the Stop -> Stop self-loop
    class ParentStations < Stops
      SCHEMA = Stops::SCHEMA.merge({
        "location_type" => Polars::Enum.new(EnumValues::STATION_LOCATION_TYPE.map(&:first)),
        "parent_station" => Polars::Null
      })

      ENUM_VALUE_MAP = Stops::ENUM_VALUE_MAP.merge({
        "location_type" => :STATION_LOCATION_TYPE
      })
    end
  end
end
