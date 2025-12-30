# frozen_string_literal: true

require "spec_helper"

RSpec.describe GtfsDf::Writer do
  require "csv"

  let(:fixture_zip) { File.expand_path("../fixtures/sample_gtfs.zip", __dir__) }
  let(:output_zip) { File.expand_path("../../fixtures/output_gtfs.zip", __dir__) }
  let(:filtered_zip) { File.expand_path("../../fixtures/filtered_gtfs.zip", __dir__) }

  after do
    FileUtils.rm_f(output_zip)
    FileUtils.rm_f(filtered_zip)
  end

  it "exports a Feed to a GTFS zip file" do
    feed = GtfsDf::Reader.load_from_zip(fixture_zip)
    GtfsDf::Writer.write_to_zip(feed, output_zip)
    expect(File.exist?(output_zip)).to be true

    # Unzip and check files
    files = []
    Zip::File.open(output_zip) { |zip| files = zip.map(&:name) }
    input_files = GtfsDf::Feed::GTFS_FILES.select { |f| feed.send(f).is_a?(Polars::DataFrame) }.map { |f| "#{f}.txt" }
    expect(files.sort).to match_array(input_files.sort)
  end

  it "exports a filtered Feed to a GTFS zip file" do
    feed = GtfsDf::Reader.load_from_zip(fixture_zip)
    # Filter: select only the first route
    route_ids = feed.routes["route_id"].to_a.first(1)
    filtered_feed = feed.filter({"routes" => {"route_id" => route_ids}})
    GtfsDf::Writer.write_to_zip(filtered_feed, filtered_zip)
    expect(File.exist?(filtered_zip)).to be true

    # Unzip and check that routes.txt only contains the filtered route
    routes_txt = nil
    Zip::File.open(filtered_zip) do |zip|
      entry = zip.find_entry("routes.txt")
      expect(entry).not_to be_nil
      routes_txt = entry.get_input_stream.read
    end

    # Check that only the filtered route_id is present
    lines = routes_txt.lines.map(&:strip)
    data_lines = lines.drop(1)
    expect(data_lines.size).to eq(1)
    expect(data_lines.first).to include(route_ids.first)
  end

  describe "round-trip data integrity" do
    let(:roundtrip_zip) { File.expand_path("../../fixtures/roundtrip_gtfs.zip", __dir__) }

    after do
      FileUtils.rm_f(roundtrip_zip)
    end

    it "preserves data through write and read cycle" do
      original_feed = GtfsDf::Reader.load_from_zip(fixture_zip)
      GtfsDf::Writer.write_to_zip(original_feed, roundtrip_zip)
      reloaded_feed = GtfsDf::Reader.load_from_zip(roundtrip_zip)

      # Compare key DataFrames
      expect(reloaded_feed.routes.height).to eq(original_feed.routes.height)
      expect(reloaded_feed.trips.height).to eq(original_feed.trips.height)
      expect(reloaded_feed.stops.height).to eq(original_feed.stops.height)
      expect(reloaded_feed.stop_times.height).to eq(original_feed.stop_times.height)

      # Compare route_ids
      expect(reloaded_feed.routes["route_id"].to_a.sort).to eq(original_feed.routes["route_id"].to_a.sort)
    end

    it "converts times back to strings when writing with parse_times enabled" do
      original_feed = GtfsDf::Reader.load_from_zip(fixture_zip, parse_times: true)

      # Verify times are integers in memory
      expect(original_feed.stop_times["arrival_time"].dtype).to eq(Polars::Int64)

      GtfsDf::Writer.write_to_zip(original_feed, roundtrip_zip)

      # Read without parsing to check the string format in the file
      string_feed = GtfsDf::Reader.load_from_zip(roundtrip_zip, parse_times: false)

      # Verify times are strings in the file
      expect(string_feed.stop_times["arrival_time"].dtype).to eq(Polars::String)
      # Verify the format matches HH:MM:SS
      string_feed.stop_times["arrival_time"].to_a.compact.first(5).each do |time|
        expect(time).to match(/^\d{2}:\d{2}:\d{2}$/)
      end
    end
  end

  describe "error handling" do
    let(:readonly_dir_zip) { "/System/readonly_test.zip" }

    it "creates output directory if it does not exist" do
      nested_output = File.expand_path("../../fixtures/nested/dir/output.zip", __dir__)
      begin
        feed = GtfsDf::Reader.load_from_zip(fixture_zip)
        GtfsDf::Writer.write_to_zip(feed, nested_output)
        expect(File.exist?(nested_output)).to be true
      ensure
        FileUtils.rm_rf(File.expand_path("../../fixtures/nested", __dir__))
      end
    end
  end

  describe ".write_to_dir" do
    let(:output_dir) { File.expand_path("../../fixtures/output_dir", __dir__) }

    after do
      FileUtils.rm_rf(output_dir)
    end

    it "exports a Feed to a directory" do
      feed = GtfsDf::Reader.load_from_zip(fixture_zip)
      GtfsDf::Writer.write_to_dir(feed, output_dir)
      expect(File.directory?(output_dir)).to be true

      input_files = GtfsDf::Feed::GTFS_FILES.select { |f| feed.send(f).is_a?(Polars::DataFrame) }.map { |f| "#{f}.txt" }
      files_in_dir = Dir.entries(output_dir).select { |f| f.end_with?(".txt") }
      expect(files_in_dir.sort).to match_array(input_files.sort)
    end

    it "converts times back to strings when writing with parse_times enabled" do
      original_feed = GtfsDf::Reader.load_from_zip(fixture_zip, parse_times: true)
      GtfsDf::Writer.write_to_dir(original_feed, output_dir)

      # Read file directly to check format
      stop_times_path = File.join(output_dir, "stop_times.txt")
      content = File.read(stop_times_path)

      # Check first five lines for time format
      csv = CSV.parse(content, headers: true)
      times = csv["arrival_time"].first(5)
      times.each do |time|
        expect(time).to match(/^\d{2}:\d{2}:\d{2}$/)
      end
    end

    it "can keep time columns as seconds since midnight" do
      original_feed = GtfsDf::Reader.load_from_zip(fixture_zip, parse_times: true)

      # Flip the option right before writing to disk to keep the integer format
      original_feed.parse_times = false
      GtfsDf::Writer.write_to_dir(original_feed, output_dir)

      # Read file directly to check format
      stop_times_path = File.join(output_dir, "stop_times.txt")
      content = File.read(stop_times_path)

      # Check first five lines for the integer format
      csv = CSV.parse(content, headers: true)
      times = csv["arrival_time"].first(5)
      times.each do |time|
        expect(time).to match(/^\d{5}$/)
      end
    end
  end
end
