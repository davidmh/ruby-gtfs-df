# frozen_string_literal: true

module GtfsDf
  class Graph
    FILES = %w[
      agency routes trips stop_times calendar calendar_dates shapes transfers frequencies fare_attributes fare_rules
      fare_leg_join_rules fare_transfer_rules areas networks route_networks location_groups location_group_stops booking_rules
      stop_areas fare_leg_rules
    ]

    STANDARD_FILE_NODES = FILES.map do |file|
      [file, {id: file, file: file, filter: nil}]
    end.to_h.freeze

    # Separate node definitions for stops and parent stations to handle the self-referential
    # relationship in stops.txt where stops reference parent stations via parent_station column.
    # This allows filtering to preserve parent stations when their child stops are referenced.
    STOP_NODES = {
      "stops" => {
        id: "stops",
        file: "stops",
        filter_attrs: {
          filter_col: "location_type",
          filter: Polars.col("location_type").is_in(
            Schema::EnumValues::STOP_LOCATION_TYPES.map(&:first)
          ) | Polars.col("location_type").is_null
        }
      },
      "parent_stations" => {
        id: "parent_stations",
        file: "stops",
        filter_attrs: {
          filter_col: "location_type",
          filter: Polars.col("location_type").is_in(
            Schema::EnumValues::STATION_LOCATION_TYPES.map(&:first)
          ) & Polars.col("location_type").is_not_null
        }
      }
    }.freeze

    NODES = STANDARD_FILE_NODES.merge(STOP_NODES).freeze

    # Returns a directed graph of GTFS file dependencies
    def self.build
      g = NetworkX::Graph.new
      NODES.keys.each { |node| g.add_node(node) }

      # TODO: Add fare_rules -> stops + test
      edges = [
        ["agency", "routes", {dependencies: [
          {"agency" => "agency_id", "routes" => "agency_id"}
        ]}],
        ["fare_attributes", "agency", {dependencies: [
          {"fare_attributes" => "agency_id",
           "agency" => "agency_id"}
        ]}],
        ["fare_attributes", "fare_rules", {dependencies: [
          {"fare_attributes" => "fare_id",
           "fare_rules" => "fare_id"}
        ]}],
        ["fare_rules", "routes", {dependencies: [
          {"fare_rules" => "route_id", "routes" => "route_id", :allow_null => true}
        ]}],
        ["routes", "trips", {dependencies: [
          {"routes" => "route_id", "trips" => "route_id"}
        ]}],
        ["trips", "stop_times", {dependencies: [
          {"trips" => "trip_id", "stop_times" => "trip_id"}
        ]}],
        ["stop_times", "stops", {dependencies: [
          {"stop_times" => "stop_id", "stops" => "stop_id"}
        ]}],
        # Self-referential edge: stops can reference parent stations (location_type=1)
        ["stops", "parent_stations", {dependencies: [
          {"stops" => "parent_station", "parent_stations" => "stop_id"}
        ]}],
        ["stops", "transfers", {dependencies: [
          {"stops" => "stop_id", "transfers" => "from_stop_id"},
          {"stops" => "stop_id", "transfers" => "to_stop_id"}
        ]}],
        ["trips", "calendar", {dependencies: [
          {"trips" => "service_id", "calendar" => "service_id"}
        ]}],
        ["trips", "calendar_dates", {dependencies: [
          {"trips" => "service_id", "calendar_dates" => "service_id"}
        ]}],
        ["trips", "shapes", {dependencies: [
          {"trips" => "shape_id", "shapes" => "shape_id"}
        ]}],
        ["trips", "frequencies", {dependencies: [
          {"trips" => "trip_id", "frequencies" => "trip_id"}
        ]}],

        # --- GTFS Extensions ---
        ["stops", "fare_leg_join_rules",
          {dependencies: [
            {"stops" => "stop_id", "fare_leg_join_rules" => "from_stop_id"},
            {"stops" => "stop_id", "fare_leg_join_rules" => "to_stop_id"}
          ]}],
        ["fare_leg_join_rules", "networks", {dependencies: [
          {"fare_leg_join_rules" => "from_network_id", "networks" => "network_id"},
          {"fare_leg_join_rules" => "to_network_id", "networks" => "network_id"}
        ]}],
        ["fare_leg_join_rules", "fare_leg_rules",
          {dependencies: [
            {"fare_leg_join_rules" => "fare_leg_rule_id", "fare_leg_rules" => "fare_leg_rule_id"}
          ]}],
        ["fare_transfer_rules", "fare_leg_rules",
          {dependencies: [
            {"fare_transfer_rules" => "from_leg_group_id", "fare_leg_rules" => "leg_group_id"},
            {"fare_transfer_rules" => "to_leg_group_id", "fare_leg_rules" => "leg_group_id"}
          ]}],
        ["fare_transfer_rules", "fare_products",
          {dependencies: [
            {"fare_transfer_rules" => "fare_product_id", "fare_products" => "fare_product_id"}
          ]}],
        ["areas", "stop_areas", {dependencies: [
          {"areas" => "area_id", "stop_areas" => "area_id"}
        ]}],
        ["stops", "areas", {dependencies: [
          {"stops" => "area_id", "areas" => "area_id"}
        ]}],
        ["areas", "fare_leg_rules", {dependencies: [
          {"areas" => "area_id", "fare_leg_rules" => "from_area_id"},
          {"areas" => "area_id", "fare_leg_rules" => "to_area_id"}
        ]}],
        ["networks", "route_networks", {dependencies: [
          {"networks" => "network_id", "route_networks" => "network_id"}
        ]}],
        ["networks", "routes", {dependencies: [
          {"networks" => "network_id", "routes" => "network_id"}
        ]}],
        ["networks", "fare_leg_rules", {dependencies: [
          {"networks" => "network_id", "fare_leg_rules" => "network_id"}
        ]}],
        ["route_networks", "routes", {dependencies: [
          {"route_networks" => "route_id", "routes" => "route_id"}
        ]}],
        ["route_networks", "networks", {dependencies: [
          {"route_networks" => "network_id", "networks" => "network_id"}
        ]}],
        ["location_groups", "location_group_stops", {dependencies: [
          {"location_groups" => "location_group_id", "location_group_stops" => "location_group_id"}
        ]}],
        ["location_groups", "stops", {dependencies: [
          {"location_groups" => "location_group_id", "stops" => "location_group_id"}
        ]}],
        ["location_group_stops", "stops", {dependencies: [
          {"location_group_stops" => "stop_id", "stops" => "stop_id"}
        ]}],
        ["stops", "location_group_stops", {dependencies: [
          {"stops" => "stop_id", "location_group_stops" => "stop_id"}
        ]}],
        ["location_group_stops", "location_groups", {dependencies: [
          {"location_group_stops" => "location_group_id", "location_groups" => "location_group_id"}
        ]}],
        ["booking_rules", "stop_times", {dependencies: [
          {"booking_rules" => "booking_rule_id", "stop_times" => "pickup_booking_rule_id"},
          {"booking_rules" => "booking_rule_id", "stop_times" => "drop_off_booking_rule_id"}
        ]}]
      ]

      edges.each do |from, to, attrs|
        g.add_edge(from, to, **attrs)
      end
      g
    end
  end
end
