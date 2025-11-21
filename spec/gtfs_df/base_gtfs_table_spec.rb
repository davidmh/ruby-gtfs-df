require "spec_helper"

RSpec.describe GtfsDf::BaseGtfsTable do
  describe "edge cases" do
    # NOTE: https://github.com/ankane/ruby-polars/issues/125
    it "can parse files with extraneous columns when column count matches schema" do
      trips_with_extra_cols_path =
        File.expand_path("../fixtures/trips_with_extra_cols.txt", __dir__)

      schema = GtfsDf::Schema::Trips.new(trips_with_extra_cols_path)
      expect(schema.valid?).to be(true)

      # Use the schema-defined dtype for all expected columns
      enum_type = Polars::Enum.new(GtfsDf::Schema::EnumValues::BIKES_ALLOWED.map(&:first))
      expect(schema.df["bikes_allowed"].dtype).to eq(enum_type)

      # Cast all extra columns as strings
      expect(schema.df["num_col"].dtype).to eq(Polars::String)
    end
  end
end
