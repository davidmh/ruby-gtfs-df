require "spec_helper"

RSpec.describe GtfsDf::Graph do
  describe ".build" do
    let(:graph) { described_class.build }

    it "includes all GTFS files as nodes" do
      expect(graph.nodes.sort).to include(*%w[agency
        routes
        trips
        stop_times
        stops
        calendar
        calendar_dates
        shapes
        transfers
        frequencies
        fare_attributes
        fare_rules
        fare_leg_join_rules
        fare_transfer_rules
        areas
        networks
        route_networks
        location_groups
        location_group_stops
        booking_rules])
    end

    it "has correct dependency attributes for key edges" do
      expect(graph.get_edge_data("agency", "routes")[:dependencies]).to eq([{"agency" => "agency_id",
                                                                             "routes" => "agency_id"}])

      expect(graph.get_edge_data("trips", "calendar")[:dependencies]).to eq([{"trips" => "service_id",
                                                                              "calendar" => "service_id"}])

      expect(graph.get_edge_data("fare_attributes", "agency")[:dependencies]).to eq([{"fare_attributes" => "agency_id",
                                                                                      "agency" => "agency_id"}])

      expect(graph.get_edge_data("fare_rules", "routes")[:dependencies]).to eq([{"fare_rules" => "route_id",
                                                                                 "routes" => "route_id",
                                                                                 :allow_null => true}])

      expect(graph.get_edge_data("stops", "transfers")[:dependencies]).to eq([{"stops" => "stop_id",
                                                                               "transfers" => "from_stop_id"},
        {"stops" => "stop_id",
         "transfers" => "to_stop_id"}])
    end

    it "does not have edges for unrelated files" do
      expect(graph.has_edge?("agency", "stop_times")).to be false
      expect(graph.has_edge?("stop_times", "agency")).to be false
    end

    it "has correct dependency attributes for new extension edges" do
      # Example: fare_leg_join_rules -> fare_leg_rules
      expect(graph.has_edge?("fare_leg_join_rules", "fare_leg_rules")).to be true
      expect(graph.get_edge_data("fare_leg_join_rules",
        "fare_leg_rules")[:dependencies]).to include({"fare_leg_join_rules" => "fare_leg_rule_id",
                                                                                "fare_leg_rules" => "fare_leg_rule_id"})

      # Example: fare_transfer_rules -> fare_products
      expect(graph.has_edge?("fare_transfer_rules", "fare_products")).to be true
      expect(graph.get_edge_data("fare_transfer_rules",
        "fare_products")[:dependencies]).to include({"fare_transfer_rules" => "fare_product_id",
                                                                               "fare_products" => "fare_product_id"})

      # Example: stops -> areas (unidirectional)
      expect(graph.has_edge?("stops", "areas")).to be true
      expect(graph.get_edge_data("stops",
        "areas")[:dependencies]).to include({"stops" => "area_id", "areas" => "area_id"})

      # Example: networks -> routes
      expect(graph.has_edge?("networks", "routes")).to be true
      expect(graph.get_edge_data("networks",
        "routes")[:dependencies]).to include({"networks" => "network_id",
                                                                        "routes" => "network_id"})

      # Example: route_networks -> routes
      expect(graph.has_edge?("route_networks", "routes")).to be true
      expect(graph.get_edge_data("route_networks",
        "routes")[:dependencies]).to include({"route_networks" => "route_id",
                                                                        "routes" => "route_id"})

      # Example: location_groups -> stops
      expect(graph.has_edge?("location_groups", "stops")).to be true
      expect(graph.get_edge_data("location_groups",
        "stops")[:dependencies]).to include({"location_groups" => "location_group_id",
                                                                       "stops" => "location_group_id"})

      # Example: location_group_stops -> stops
      expect(graph.has_edge?("location_group_stops", "stops")).to be true
      expect(graph.get_edge_data("location_group_stops",
        "stops")[:dependencies]).to include({"location_group_stops" => "stop_id",
                                                                       "stops" => "stop_id"})

      # Example: stop_times -> booking_rules
      expect(graph.has_edge?("booking_rules", "stop_times")).to be true
      expect(graph.get_edge_data("booking_rules",
        "stop_times")[:dependencies]).to include(
          {"booking_rules" => "booking_rule_id", "stop_times" => "pickup_booking_rule_id"}
        )
    end
  end
end
