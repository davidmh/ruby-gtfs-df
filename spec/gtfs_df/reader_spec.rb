require "spec_helper"

RSpec.describe GtfsDf::Reader do
  let(:zip_path) { File.expand_path("../fixtures/sample_gtfs.zip", __dir__) }

  shared_examples_for "feed object" do
    it "loads a GTFS zip and returns a Feed object" do
      expect(feed).to be_a(GtfsDf::Feed)
      expect(feed.routes).to be_a(Polars::DataFrame)
      expect(feed.trips).to be_a(Polars::DataFrame)

      expect(feed.trips.head.to_a.first).to match(
        {"block_id" => "1",
         "direction_id" => "0",
         "route_id" => "AB",
         "service_id" => "FULLW",
         "shape_id" => nil,
         "trip_headsign" => "to Bullfrog",
         "trip_id" => "AB1"}
      )
    end

    it "loads feed ignoring extra non-GTFS files in zip" do
      # The sample_gtfs.zip may contain extra files, ensure it doesn't break
      expect(feed).to be_a(GtfsDf::Feed)
      expect(feed.routes).to be_a(Polars::DataFrame)
    end
  end

  describe ".load_from_zip" do
    let(:feed) { described_class.load_from_zip(zip_path) }

    it_behaves_like "feed object"

    it "raises error when zip file does not exist" do
      expect do
        described_class.load_from_zip("/nonexistent/path/to/file.zip")
      end.to raise_error(Zip::Error)
    end

    it "filters trips by service_id FULLW from the GTFS zip fixture" do
      expect(feed.trips["service_id"].to_a.uniq).to eq(%w[FULLW WE])
      expect(feed.calendar["service_id"].to_a.uniq).to eq(%w[FULLW WE])

      filtered = feed.filter({"trips" => {"service_id" => "FULLW"}})

      expect(filtered.trips["service_id"].to_a.uniq).to eq(["FULLW"])
      expect(filtered.calendar["service_id"].to_a.uniq).to eq(["FULLW"])
    end
  end

  describe ".load_from_dir" do
    let(:feed) do
      Dir.mktmpdir do |tmp_dir|
        Zip::File.open(zip_path) do |zipfile|
          zipfile.each { |entry|
            entry.extract(File.join(tmp_dir, entry.name))
          }
        end

        described_class.load_from_dir(tmp_dir)
      end
    end

    it_behaves_like "feed object"
  end
end
