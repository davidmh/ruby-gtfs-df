# frozen_string_literal: true

module GtfsDf
  class Graph
    # Returns a directed graph of GTFS file dependencies
    def self.build
      g = NetworkX::Graph.new
      # Nodes: GTFS files
      files = %w[
        agency routes trips stop_times stops calendar calendar_dates shapes transfers frequencies fare_attributes fare_rules
        fare_leg_join_rules fare_transfer_rules areas networks route_networks location_groups location_group_stops booking_rules
      ]
      files.each { |f| g.add_node(f) }

      # TODO: Add fare_rules -> stops + test
      edges = [
        ["agency", "routes", {dependencies: [
          {"agency" => "agency_id", "routes" => "agency_id"}
        ]}],
        ["fare_attributes", "fare_rules", {dependencies: [
          {"fare_attributes" => "fare_id",
           "fare_rules" => "fare_id"}
        ]}],
        ["fare_rules", "routes", {dependencies: [
          {"fare_rules" => "route_id", "routes" => "route_id"}
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
        ]}],
        ["stops", "booking_rules", {dependencies: [
          {"stops" => "stop_id", "booking_rules" => "stop_id"}
        ]}]
      ]

      edges.each do |from, to, attrs|
        g.add_edge(from, to, **attrs)
      end
      g
    end
  end
end
