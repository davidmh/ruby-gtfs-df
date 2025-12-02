require "spec_helper"

RSpec.describe GtfsDf::Feed do
  let(:agency_df) do
    Polars::DataFrame.new({"agency_id" => ["A"],
                            "agency_name" => ["Test Agency"],
                            "agency_url" => ["http://agency"],
                            "agency_timezone" => ["America/Chicago"]})
  end
  let(:stops_df) do
    Polars::DataFrame.new({"stop_id" => %w[S1 S2],
                            "stop_name" => %w[Stop1 Stop2],
                            "stop_lat" => %w[0 1],
                            "stop_lon" => %w[0 1]})
  end
  let(:routes_df) do
    Polars::DataFrame.new({"route_id" => %w[1 2],
                            "agency_id" => %w[A A],
                            "route_short_name" => %w[A B]})
  end
  let(:trips_df) do
    Polars::DataFrame.new({"trip_id" => %w[t1 t2],
                            "route_id" => %w[1 2],
                            "service_id" => %w[A B]})
  end
  let(:stop_times_df) do
    Polars::DataFrame.new({"trip_id" => %w[t1 t2],
                            "stop_id" => %w[S1 S2],
                            "stop_sequence" => [1, 1]})
  end
  let(:calendar_df) do
    Polars::DataFrame.new({"service_id" => %w[A B],
                            "monday" => %w[1 1],
                            "tuesday" => %w[1 1],
                            "wednesday" => %w[1 1],
                            "thursday" => %w[1 1],
                            "friday" => %w[1 1],
                            "saturday" => %w[1 1],
                            "sunday" => %w[1 1],
                            "start_date" => %w[20250101 20250101],
                            "end_date" => %w[20251231 20251231]})
  end
  let(:feed) do
    described_class.new(
      "agency" => agency_df,
      "stops" => stops_df,
      "routes" => routes_df,
      "trips" => trips_df,
      "stop_times" => stop_times_df,
      "calendar" => calendar_df
    )
  end

  describe ".new" do
    it "sets GTFS file properties as DataFrames" do
      expect(feed.routes).to eq(routes_df)
      expect(feed.trips).to eq(trips_df)
    end
  end

  describe ".filter" do
    describe "basic functionality" do
      it "filters DataFrames using the view hash" do
        expect(feed.routes["route_id"].to_a).to eq(%w[1 2])
        expect(feed.trips["service_id"].to_a).to eq(%w[A B])

        view = {"routes" => {"route_id" => "1"}}
        filtered = feed.filter(view)

        expect(filtered.routes["route_id"].to_a).to eq(["1"])
        expect(filtered.trips["service_id"].to_a).to eq(%w[A])
      end

      it "filters DataFrames using multiple values" do
        view = {"routes" => {"route_id" => %w[1 2]}}
        filtered = feed.filter(view)
        expect(filtered.routes["route_id"].to_a).to eq(%w[1 2])
        expect(filtered.trips["route_id"].to_a).to eq(%w[1 2])
      end

      it "filters DataFrames using a custom predicate" do
        view = {"routes" => {"route_id" => ->(col) { col.str.starts_with("1") }}}
        filtered = feed.filter(view)
        expect(filtered.routes["route_id"].to_a).to eq(["1"])
        expect(filtered.trips["route_id"].to_a).to eq(["1"])
      end
    end

    describe "filtering through the graph" do
      let(:agency_df) do
        Polars::DataFrame.new({"agency_id" => ["A", "B"],
                                "agency_name" => ["Test A", "Test B"],
                                "agency_url" => ["http://agencyA", "http://agencyB"],
                                "agency_timezone" => ["America/Chicago", "America/Chicago"]})
      end
      let(:routes_df) do
        Polars::DataFrame.new({"route_id" => %w[1 2],
                                "agency_id" => %w[A B],
                                "route_short_name" => %w[A B]})
      end
      # S5 is the parent station for S1
      # S6 is the parent station for S4
      let(:stops_df) do
        Polars::DataFrame.new({"stop_id" => %w[S1 S2 S3 S4 S5 S6],
                                "stop_name" => %w[Stop1 Stop2 Stop3 Stop4 Station1 Station2],
                                "stop_lat" => %w[0 1 2 3 0 0],
                                "stop_lon" => %w[0 1 2 3 0 0],
                                "parent_station" => ["S5", nil, nil, "S6", nil, nil],
                                "location_type" => %w[0 0 0 0 1 1]})
      end
      # Trip 1 visits stops 1,2,3
      # Trip 2 visits stops 2,4
      let(:stop_times_df) do
        Polars::DataFrame.new({"trip_id" => %w[t1 t1 t1 t2 t2],
                                "stop_id" => %w[S1 S2 S3 S2 S4],
                                "stop_sequence" => [1, 2, 3, 1, 2]})
      end

      it "filtering from trips cascades" do
        # We consider trips the "atomic unit" of GTFS
        view = {"trips" => {"trip_id" => %w[t1]}}
        filtered = feed.filter(view)

        # When trips are filtered, we expect the filters to propagate to stop_times
        expect(filtered.stop_times["stop_id"].to_a).to eq(%w[S1 S2 S3])

        # Remove unreferenced objects
        expect(filtered.stops["stop_id"].to_a).to match_array(%w[S1 S2 S3 S5])
        expect(filtered.routes["route_id"].to_a).to eq(%w[1])
        expect(filtered.agency["agency_id"].to_a).to eq(%w[A])
        expect(filtered.calendar["service_id"].to_a).to eq(%w[A])
      end

      it "filtering from stop cascades" do
        # We consider trips the "atomic unit" of GTFS
        # This means if we filter to a stop, we should essentially filter to all trips
        # which visit that stop and retain referential integrity for those trips
        # This stop is found only in T1
        view = {"stops" => {"stop_id" => %w[S1]}}
        filtered = feed.filter(view)

        # Retain all stop times for T1 but not T2
        expect(filtered.stop_times["stop_id"].to_a).to eq(%w[S1 S2 S3])

        # Retain all stops referenced by T1 T1
        expect(filtered.stops["stop_id"].to_a).to match_array(%w[S1 S2 S3 S5])
        expect(filtered.routes["route_id"].to_a).to eq(%w[1])
        expect(filtered.agency["agency_id"].to_a).to eq(%w[A])
        expect(filtered.calendar["service_id"].to_a).to eq(%w[A])
      end

      it "filtering from agency cascades" do
        # Filtering by agency should cascade to routes -> trips -> etc.
        view = {"agency" => {"agency_id" => %w[A]}}
        filtered = feed.filter(view)

        expect(filtered.agency["agency_id"].to_a).to eq(%w[A])

        expect(filtered.routes["route_id"].to_a).to eq(%w[1])
        expect(filtered.trips["trip_id"].to_a).to eq(%w[t1])
        expect(filtered.stop_times["stop_id"].to_a).to eq(%w[S1 S2 S3])

        # Remove unreferenced objects
        expect(filtered.stops["stop_id"].to_a).to match_array(%w[S1 S2 S3 S5])
        expect(filtered.calendar["service_id"].to_a).to eq(%w[A])
      end

      it "filtering from calendar cascades" do
        # Filtering by agency should cascade to routes -> trips -> etc.
        view = {"calendar" => {"service_id" => %w[A]}}
        filtered = feed.filter(view)

        expect(filtered.calendar["service_id"].to_a).to eq(%w[A])

        # Only keep trips and stop times for this service
        expect(filtered.trips["trip_id"].to_a).to eq(%w[t1])
        expect(filtered.stop_times["stop_id"].to_a).to eq(%w[S1 S2 S3])

        # Remove unreferenced objects
        expect(filtered.stops["stop_id"].to_a).to match_array(%w[S1 S2 S3 S5])
        expect(filtered.routes["route_id"].to_a).to eq(%w[1])
        expect(filtered.agency["agency_id"].to_a).to eq(%w[A])
      end

      # TODO: this expected behavior is not yet supported
      xit "filtering from a parent station cascades" do
        # This stop is the station for trip 1. However, since we enter the graph
        # through the stop node, we search for a stop_time with this stop and find
        # nothing.
        view = {"stops" => {"stop_id" => %w[S5]}}
        filtered = feed.filter(view)

        # Retain all stop times for T1 but not T2
        expect(filtered.stop_times["stop_id"].to_a).to eq(%w[S1 S2 S3])

        # Retain all stops referenced by T1 T1
        expect(filtered.stops["stop_id"].to_a).to match_array(%w[S1 S2 S3 S5])
        expect(filtered.routes["route_id"].to_a).to eq(%w[1])
        expect(filtered.agency["agency_id"].to_a).to eq(%w[A])
        expect(filtered.calendar["service_id"].to_a).to eq(%w[A])
      end
    end

    describe "edge cases" do
      let(:feed) do
        described_class.new({"agency" => agency_df,
                              "stops" => stops_df,
                              "routes" => routes_df,
                              "trips" => trips_df,
                              "stop_times" => stop_times_df,
                              "calendar" => calendar_df})
      end

      it "returns empty feed when filter matches nothing" do
        view = {"routes" => {"route_id" => "NONEXISTENT"}}
        filtered = feed.filter(view)
        expect(filtered.routes.height).to eq(0)
        # Trips and their stop_times should cascade and be empty since no valid routes exist
        expect(filtered.trips.height).to eq(0)
        expect(filtered.stop_times.height).to eq(0)
        # No trips reference this calendar so deletion is cascaded
        expect(filtered.calendar.height).to eq(0)
        # No trips reference this agency so deletion is cascaded
        expect(filtered.agency.height).to eq(0)
      end

      it "handles filtering with empty array" do
        view = {"routes" => {"route_id" => []}}
        filtered = feed.filter(view)
        expect(filtered.routes.height).to eq(0)
      end
    end
  end

  describe "initialization error handling" do
    let(:minimal_feed_data) do
      {
        "agency" => Polars::DataFrame.new({"agency_id" => ["A"],
                                            "agency_name" => ["Test"],
                                            "agency_url" => ["http://test.com"],
                                            "agency_timezone" => ["America/Chicago"]}),
        "stops" => Polars::DataFrame.new({"stop_id" => ["S1"],
                                           "stop_name" => ["Stop 1"],
                                           "stop_lat" => ["0"],
                                           "stop_lon" => ["0"]}),
        "routes" => Polars::DataFrame.new({"route_id" => ["R1"],
                                            "route_short_name" => ["A"]}),
        "trips" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                           "route_id" => ["R1"],
                                           "service_id" => ["S1"]}),
        "stop_times" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                                "stop_id" => ["S1"],
                                                "stop_sequence" => [1]}),
        "calendar" => Polars::DataFrame.new({"service_id" => ["S1"],
                                              "monday" => ["1"],
                                              "tuesday" => ["1"],
                                              "wednesday" => ["1"],
                                              "thursday" => ["1"],
                                              "friday" => ["1"],
                                              "saturday" => ["1"],
                                              "sunday" => ["1"],
                                              "start_date" => ["20250101"],
                                              "end_date" => ["20251231"]})
      }
    end

    it "raises error when missing agency file" do
      data = minimal_feed_data.dup
      data.delete("agency")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*agency.txt/)
    end

    it "raises error when missing stops file" do
      data = minimal_feed_data.dup
      data.delete("stops")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*stops.txt/)
    end

    it "raises error when missing routes file" do
      data = minimal_feed_data.dup
      data.delete("routes")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*routes.txt/)
    end

    it "raises error when missing trips file" do
      data = minimal_feed_data.dup
      data.delete("trips")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*trips.txt/)
    end

    it "raises error when missing stop_times file" do
      data = minimal_feed_data.dup
      data.delete("stop_times")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*stop_times.txt/)
    end

    it "raises error when missing both calendar and calendar_dates" do
      data = minimal_feed_data.dup
      data.delete("calendar")
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*calendar/)
    end

    it "accepts calendar_dates instead of calendar" do
      data = minimal_feed_data.dup
      data.delete("calendar")
      data["calendar_dates"] = Polars::DataFrame.new({"service_id" => ["S1"],
                                                       "date" => ["20250101"],
                                                       "exception_type" => ["1"]})
      expect { described_class.new(data) }.not_to raise_error
    end

    it "accepts both calendar and calendar_dates" do
      data = minimal_feed_data.dup
      data["calendar_dates"] = Polars::DataFrame.new({"service_id" => ["S1"],
                                                       "date" => ["20250101"],
                                                       "exception_type" => ["1"]})
      expect { described_class.new(data) }.not_to raise_error
    end

    it "raises error when agency is not a DataFrame" do
      data = minimal_feed_data.dup
      data["agency"] = "not a dataframe"
      expect { described_class.new(data) }.to raise_error(GtfsDf::Error, /Missing required GTFS files.*agency.txt/)
    end
  end

  describe "GTFS extension file support" do
    let(:minimal_feed_data) do
      {
        "agency" => Polars::DataFrame.new({"agency_id" => ["A"],
                                            "agency_name" => ["Test"],
                                            "agency_url" => ["http://test.com"],
                                            "agency_timezone" => ["America/Chicago"]}),
        "stops" => Polars::DataFrame.new({"stop_id" => ["S1"],
                                           "stop_name" => ["Stop 1"],
                                           "stop_lat" => ["0"],
                                           "stop_lon" => ["0"]}),
        "routes" => Polars::DataFrame.new({"route_id" => ["R1"],
                                            "route_short_name" => ["A"]}),
        "trips" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                           "route_id" => ["R1"],
                                           "service_id" => ["S1"]}),
        "stop_times" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                                "stop_id" => ["S1"],
                                                "stop_sequence" => [1]}),
        "calendar" => Polars::DataFrame.new({"service_id" => ["S1"],
                                              "monday" => ["1"],
                                              "tuesday" => ["1"],
                                              "wednesday" => ["1"],
                                              "thursday" => ["1"],
                                              "friday" => ["1"],
                                              "saturday" => ["1"],
                                              "sunday" => ["1"],
                                              "start_date" => ["20250101"],
                                              "end_date" => ["20251231"]})
      }
    end

    shared_examples "loads extension file" do |file_name, sample_data, required_columns, id_column|
      it "loads #{file_name} DataFrame with required fields" do
        feed_data = minimal_feed_data.merge(file_name => Polars::DataFrame.new(sample_data))
        feed = GtfsDf::Feed.new(feed_data)

        expect(feed.send(file_name)).to be_a(Polars::DataFrame)
        expect(feed.send(file_name).columns).to include(*required_columns)
        expect(feed.send(file_name)[id_column].to_a).to eq([sample_data[id_column].first])
      end
    end

    include_examples "loads extension file", "fare_leg_join_rules",
      {"fare_leg_join_rule_id" => ["FLJR1"],
       "from_leg_group_id" => ["LG1"],
       "to_leg_group_id" => ["LG2"],
       "transfer_count" => [1],
       "duration_limit" => [1800]},
      %w[fare_leg_join_rule_id from_leg_group_id to_leg_group_id transfer_count duration_limit],
      "fare_leg_join_rule_id"

    include_examples "loads extension file", "fare_transfer_rules",
      {"fare_transfer_rule_id" => ["FTR1"],
       "from_leg_group_id" => ["LG1"],
       "to_leg_group_id" => ["LG2"],
       "transfer_count" => [2],
       "duration_limit" => [3600]},
      %w[fare_transfer_rule_id from_leg_group_id to_leg_group_id transfer_count duration_limit],
      "fare_transfer_rule_id"

    include_examples "loads extension file", "areas",
      {"area_id" => ["A1"],
       "area_name" => ["Zone 1"],
       "area_type" => ["zone"]},
      %w[area_id area_name area_type],
      "area_id"

    include_examples "loads extension file", "networks",
      {"network_id" => ["N1"],
       "network_name" => ["Metro"],
       "network_url" => ["http://metro.com"]},
      %w[network_id network_name network_url],
      "network_id"

    include_examples "loads extension file", "route_networks",
      {"route_id" => ["R1"],
       "network_id" => ["N1"]},
      %w[route_id network_id],
      "route_id"

    include_examples "loads extension file", "location_groups",
      {"location_group_id" => ["LG1"],
       "location_group_name" => ["Group 1"]},
      %w[location_group_id location_group_name],
      "location_group_id"

    include_examples "loads extension file", "location_group_stops",
      {"location_group_id" => ["LG1"],
       "stop_id" => ["S1"]},
      %w[location_group_id stop_id],
      "location_group_id"

    include_examples "loads extension file", "booking_rules",
      {"booking_rule_id" => ["BR1"],
       "booking_method" => ["phone"],
       "info_url" => ["http://booking.com"]},
      %w[booking_rule_id booking_method info_url],
      "booking_rule_id"
  end

  describe "filtering cascades to extension files" do
    it "filters areas, location_groups, location_group_stops, and booking_rules when stops are filtered" do
      feed = GtfsDf::Feed.new({"agency" => Polars::DataFrame.new({"agency_id" => ["A"],
                                                                    "agency_name" => ["Agency"],
                                                                    "agency_url" => ["http://agency"],
                                                                    "agency_timezone" => ["America/Chicago"]}),
                                "routes" => Polars::DataFrame.new({"route_id" => ["R1"],
                                                                    "agency_id" => ["A"],
                                                                    "route_short_name" => ["Route"]}),
                                "trips" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                                                   "route_id" => ["R1"],
                                                                   "service_id" => ["S1"]}),
                                "stop_times" => Polars::DataFrame.new({"trip_id" => ["T1"],
                                                                        "stop_id" => ["S1"],
                                                                        "stop_sequence" => [1],
                                                                        "pickup_booking_rule_id" => %w[BR1]}),
                                "calendar" => Polars::DataFrame.new({"service_id" => ["S1"],
                                                                      "monday" => ["1"],
                                                                      "tuesday" => ["1"],
                                                                      "wednesday" => ["1"],
                                                                      "thursday" => ["1"],
                                                                      "friday" => ["1"],
                                                                      "saturday" => ["1"],
                                                                      "sunday" => ["1"],
                                                                      "start_date" => ["20250101"],
                                                                      "end_date" => ["20251231"]}),
                                "stops" => Polars::DataFrame.new({"stop_id" => %w[S1 S2],
                                                                   "stop_name" => ["Stop 1", "Stop 2"],
                                                                   "area_id" => %w[A1 A2],
                                                                   "location_group_id" => %w[LG1 LG2]}),
                                "areas" => Polars::DataFrame.new({"area_id" => %w[A1 A2],
                                                                   "area_name" => ["Zone 1", "Zone 2"],
                                                                   "area_type" => %w[zone zone]}),
                                "location_groups" => Polars::DataFrame.new({"location_group_id" => %w[LG1 LG2],
                                                                             "location_group_name" => ["Group 1",
                                                                               "Group 2"]}),
                                "location_group_stops" => Polars::DataFrame.new({"location_group_id" => %w[LG1 LG2],
                                                                                  "stop_id" => %w[S1 S2]}),
                                "booking_rules" => Polars::DataFrame.new({"booking_rule_id" => %w[BR1 BR2],
                                                                           "booking_method" => %w[phone web],
                                                                           "info_url" => ["http://a", "http://b"]})})
      filtered = feed.filter({"stops" => {stop_id: ["S1"]}})
      expect(filtered.stops["stop_id"].to_a).to eq(["S1"])
      expect(filtered.areas["area_id"].to_a).to eq(["A1"])
      expect(filtered.location_groups["location_group_id"].to_a).to eq(["LG1"])
      expect(filtered.location_group_stops["stop_id"].to_a).to eq(["S1"])
      expect(filtered.booking_rules["booking_rule_id"].to_a).to eq(["BR1"])
    end
  end
end
