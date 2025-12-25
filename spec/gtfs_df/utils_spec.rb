require "spec_helper"

RSpec.describe GtfsDf::Utils do
  describe "GTFS time parsing" do
    it "parses standard GTFS times correctly" do
      expect(described_class.parse_time("08:30:00")).to eq(8 * 3600 + 30 * 60)
      expect(described_class.parse_time("00:00:00")).to eq(0)
      expect(described_class.parse_time("23:59:59")).to eq(23 * 3600 + 59 * 60 + 59)
    end

    it "parses over-24-hour GTFS times correctly" do
      expect(described_class.parse_time("25:10:00")).to eq(25 * 3600 + 10 * 60)
      expect(described_class.parse_time("36:00:00")).to eq(36 * 3600)
    end

    it "parses times at boundaries correctly" do
      expect(described_class.parse_time("24:00:00")).to eq(24 * 3600)
      expect(described_class.parse_time("00:00:01")).to eq(1)
      expect(described_class.parse_time("23:59:58")).to eq(23 * 3600 + 59 * 60 + 58)
    end

    it "parses times with or without leading zeros" do
      expect(described_class.parse_time("8:30:00")).to eq(8 * 3600 + 30 * 60)
      expect(described_class.parse_time("08:30:00")).to eq(8 * 3600 + 30 * 60)
    end

    it "returns integer seconds as-is" do
      expect(described_class.parse_time(3600)).to eq(3600)
      expect(described_class.parse_time(0)).to eq(0)
    end

    it "returns nil for nil, empty, or malformed times" do
      expect(described_class.parse_time(nil)).to be_nil
      expect(described_class.parse_time("")).to be_nil
      expect(described_class.parse_time("not_a_time")).to be_nil
      expect(described_class.parse_time("12:34")).to be_nil
      expect(described_class.parse_time("12:34:")).to be_nil
      expect(described_class.parse_time("12:34:xx")).to be_nil
    end

    it "handles whitespace appropriately" do
      expect(described_class.parse_time("  ")).to be_nil
      expect(described_class.parse_time("\t\n")).to be_nil
    end
  end

  describe "GTFS time formatting" do
    it "formats standard times correctly" do
      expect(described_class.format_time(8 * 3600 + 30 * 60)).to eq("08:30:00")
      expect(described_class.format_time(0)).to eq("00:00:00")
      expect(described_class.format_time(23 * 3600 + 59 * 60 + 59)).to eq("23:59:59")
    end

    it "formats over-24-hour times correctly" do
      expect(described_class.format_time(25 * 3600 + 10 * 60)).to eq("25:10:00")
      expect(described_class.format_time(36 * 3600)).to eq("36:00:00")
      expect(described_class.format_time(25 * 3600 + 35 * 60)).to eq("25:35:00")
    end

    it "formats times at boundaries correctly" do
      expect(described_class.format_time(24 * 3600)).to eq("24:00:00")
      expect(described_class.format_time(1)).to eq("00:00:01")
      expect(described_class.format_time(3661)).to eq("01:01:01")
    end

    it "returns string times as-is" do
      expect(described_class.format_time("08:30:00")).to eq("08:30:00")
    end

    it "returns nil for nil" do
      expect(described_class.format_time(nil)).to be_nil
    end

    it "round-trips correctly" do
      ["08:30:00", "00:00:00", "23:59:59", "25:35:00", "36:00:00"].each do |time_str|
        seconds = described_class.parse_time(time_str)
        formatted = described_class.format_time(seconds)
        expect(formatted).to eq(time_str)
      end
    end
  end

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

  describe "GTFS date parsing" do
    it "parses valid GTFS dates correctly" do
      expect(described_class.parse_date("20180913")).to eq(Date.new(2018, 9, 13))
      expect(described_class.parse_date("20200101")).to eq(Date.new(2020, 1, 1))
    end

    it "parses boundary dates correctly" do
      expect(described_class.parse_date("20201231")).to eq(Date.new(2020, 12, 31))
      expect(described_class.parse_date("20210101")).to eq(Date.new(2021, 1, 1))
    end

    it "parses leap year dates correctly" do
      expect(described_class.parse_date("20200229")).to eq(Date.new(2020, 2, 29))
      expect(described_class.parse_date("20240229")).to eq(Date.new(2024, 2, 29))
    end

    it "returns nil for nil, empty, or malformed dates" do
      expect(described_class.parse_date(nil)).to be_nil
      expect(described_class.parse_date("")).to be_nil
      expect(described_class.parse_date("not_a_date")).to be_nil
      expect(described_class.parse_date("2020-01-01")).to be_nil
      expect(described_class.parse_date("2020011")).to be_nil # too short
      expect(described_class.parse_date("202001011")).to be_nil # too long
    end

    it "returns nil for invalid dates" do
      expect(described_class.parse_date("20230230")).to be_nil # Feb 30 does not exist
      expect(described_class.parse_date("20211301")).to be_nil # Month 13 does not exist
      expect(described_class.parse_date("20210001")).to be_nil # Month 00 does not exist
      expect(described_class.parse_date("20211232")).to be_nil # Day 32 does not exist
    end

    it "returns nil for non-leap year Feb 29" do
      expect(described_class.parse_date("20190229")).to be_nil
      expect(described_class.parse_date("20230229")).to be_nil
    end

    it "handles whitespace appropriately" do
      expect(described_class.parse_date("  ")).to be_nil
      expect(described_class.parse_date("\t\n")).to be_nil
    end
  end
end
