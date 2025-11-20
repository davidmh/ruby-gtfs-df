require "spec_helper"
require "byebug"

RSpec.describe GtfsDf::BaseGtfsTable do
  describe "edge cases" do
    # NOTE: https://github.com/ankane/ruby-polars/issues/125
    it "can parse files with extraneous columns when column count matches schema" do
      trips_with_extra_cols_path =
        File.expand_path("../fixtures/trips_with_extra_cols.txt", __dir__)

      schema = GtfsDf::Schema::Trips.new(trips_with_extra_cols_path)
      expect(schema.valid?).to be(true)
    end
  end
end
