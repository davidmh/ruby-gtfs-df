require "spec_helper"

RSpec.describe GtfsDf::Utils do
  describe "Polars time conversion" do
    it "converts time strings to seconds in a DataFrame" do
      df = Polars::DataFrame.new({
        "time" => ["08:30:00", "14:30:00", "25:35:00"]
      })

      result = df.with_columns(
        described_class.as_seconds_since_midnight("time").alias("seconds")
      )

      expect(result["seconds"].to_a).to eq([
        8 * 3600 + 30 * 60,
        14 * 3600 + 30 * 60,
        25 * 3600 + 35 * 60
      ])
    end

    it "converts seconds to time strings in a DataFrame" do
      df = Polars::DataFrame.new({
        "seconds" => [
          8 * 3600 + 30 * 60,
          14 * 3600 + 30 * 60,
          25 * 3600 + 35 * 60
        ]
      })

      result = df.with_columns(
        described_class.as_time_string("seconds").alias("time")
      )

      expect(result["time"].to_a).to eq([
        "08:30:00",
        "14:30:00",
        "25:35:00"
      ])
    end

    it "round-trips time conversion correctly" do
      original_df = Polars::DataFrame.new({
        "time" => ["08:30:00", "14:30:00", "25:35:00", "00:00:00", "23:59:59"]
      })

      # Convert to seconds and back
      result = original_df.with_columns(
        described_class.as_seconds_since_midnight("time").alias("seconds")
      ).with_columns(
        described_class.as_time_string("seconds").alias("time_converted")
      )

      expect(result["time_converted"].to_a).to eq(result["time"].to_a)
    end
  end

  describe "Polars date parsing" do
    def parse_examples(dates)
      df = Polars::DataFrame.new({"example" => dates})
      df.select(described_class.parse_date(Polars.col("example")))["example"].to_a
    end

    it "parses valid GTFS dates correctly" do
      expect(parse_examples(["20180913", "20200101"])).to match([
        Date.new(2018, 9, 13),
        Date.new(2020, 1, 1)
      ])
    end

    it "parses boundary dates correctly" do
      expect(parse_examples(["20201231", "20210101"])).to eq([
        Date.new(2020, 12, 31),
        Date.new(2021, 1, 1)
      ])
    end

    it "parses leap year dates correctly" do
      expect(parse_examples(["20200229", "20240229"])).to eq([
        Date.new(2020, 2, 29),
        Date.new(2024, 2, 29)
      ])
    end

    it "returns nil for nil, empty, or invalid dates" do
      expect(parse_examples([
        nil,
        "",
        "  ",
        "\t\n",
        "not_a_date",
        "2020-01-01", # with dashes
        "202001011", # too long
        "20230230", # Feb 30 does not exist
        "20211301", # Month 13 does not exist
        "20210001", # Month 00 does not exist
        "20211232", # Day 32 does not exist
        "20190229" # Non-leap year Feb 29
      ]).compact).to be_empty
    end

    it "supports non-standard formats" do
      expect(parse_examples([
        "2020011" # too short
      ])).to match([Date.new(2020, 1, 1)])
    end
  end
end
