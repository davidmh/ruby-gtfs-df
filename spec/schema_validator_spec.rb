# frozen_string_literal: true

require 'spec_helper'

RSpec.describe GtfsDf::SchemaValidator do
  let(:valid_trips_df) do
    Polars::DataFrame.new({ 'route_id' => ['R1'],
                            'service_id' => ['S1'],
                            'trip_id' => ['T1'],
                            'direction_id' => ['1'],
                            'wheelchair_accessible' => ['0'],
                            'bikes_allowed' => ['2'],
                            'cars_allowed' => ['1'] })
  end

  let(:invalid_trips_df) do
    Polars::DataFrame.new({ 'route_id' => ['R1'],
                            'service_id' => ['S1'],
                            'trip_id' => ['T1'],
                            'direction_id' => ['3'], # invalid
                            'wheelchair_accessible' => ['0'],
                            'bikes_allowed' => ['2'],
                            'cars_allowed' => ['1'] })
  end

  let(:valid_stop_times_df) do
    Polars::DataFrame.new({ 'trip_id' => ['T1'],
                            'stop_sequence' => [1],
                            'stop_id' => ['S1'],
                            'pickup_type' => ['0'],
                            'drop_off_type' => ['1'],
                            'continuous_pickup' => ['2'],
                            'continuous_drop_off' => ['3'],
                            'timepoint' => ['1'] })
  end

  let(:invalid_stop_times_df) do
    Polars::DataFrame.new({ 'trip_id' => ['T1'],
                            'stop_sequence' => [1],
                            'stop_id' => ['S1'],
                            'pickup_type' => ['5'], # invalid
                            'drop_off_type' => ['1'],
                            'continuous_pickup' => ['2'],
                            'continuous_drop_off' => ['3'],
                            'timepoint' => ['1'] })
  end

  let(:valid_calendar_df) do
    Polars::DataFrame.new({ 'service_id' => ['S1'],
                            'monday' => ['1'],
                            'tuesday' => ['0'],
                            'wednesday' => ['1'],
                            'thursday' => ['0'],
                            'friday' => ['1'],
                            'saturday' => ['0'],
                            'sunday' => ['1'],
                            'start_date' => ['20250101'],
                            'end_date' => ['20251231'] })
  end

  let(:invalid_calendar_df) do
    Polars::DataFrame.new({ 'service_id' => ['S1'],
                            'monday' => ['2'], # invalid
                            'tuesday' => ['0'],
                            'wednesday' => ['1'],
                            'thursday' => ['0'],
                            'friday' => ['1'],
                            'saturday' => ['0'],
                            'sunday' => ['1'],
                            'start_date' => ['20250101'],
                            'end_date' => ['20251231'] })
  end

  it 'validates allowed enum values for Trips' do
    validator = described_class.new(valid_trips_df, GtfsDf::Schema::Trips)
    expect(validator.valid?).to be true
    validator = described_class.new(invalid_trips_df, GtfsDf::Schema::Trips)
    expect(validator.valid?).to be false
    expect(validator.errors.any? { |e| e.include?('direction_id') }).to be true
  end

  it 'validates allowed enum values for StopTimes' do
    validator = described_class.new(valid_stop_times_df, GtfsDf::Schema::StopTimes)
    expect(validator.valid?).to be true
    validator = described_class.new(invalid_stop_times_df, GtfsDf::Schema::StopTimes)
    expect(validator.valid?).to be false
    expect(validator.errors.any? { |e| e.start_with?('pickup_type') }).to be true
  end

  it 'validates allowed enum values for Calendar' do
    validator = described_class.new(valid_calendar_df, GtfsDf::Schema::Calendar)
    expect(validator.valid?).to be true
    validator = described_class.new(invalid_calendar_df, GtfsDf::Schema::Calendar)
    expect(validator.valid?).to be false
    expect(validator.errors.any? { |e| e.include?('monday') }).to be true
  end

  it 'does not materialize LazyFrame during type validation' do
    class DummySchema
      REQUIRED_FIELDS = %w[foo bar]
      SCHEMA = {
        'foo' => Polars::String,
        'bar' => Polars::Int64
      }
    end

    # Create a LazyFrame
    lf = Polars::DataFrame.new({ 'foo' => %w[a b],
                                 'bar' => [1, 2] }).lazy

    validator = GtfsDf::SchemaValidator.new(lf, DummySchema)
    expect(validator.valid?).to be true
    # Assert that the frame is still a LazyFrame
    expect(lf).to be_a(Polars::LazyFrame)
  end

  describe 'edge cases' do
    it 'validates empty DataFrames' do
      empty_df = Polars::DataFrame.new({ 'route_id' => [], 'service_id' => [], 'trip_id' => [] })
      validator = described_class.new(empty_df, GtfsDf::Schema::Trips)
      expect(validator.valid?).to be true
    end

    it 'detects missing required columns' do
      incomplete_df = Polars::DataFrame.new({ 'route_id' => ['R1'] })
      validator = described_class.new(incomplete_df, GtfsDf::Schema::Trips)
      expect(validator.valid?).to be false
      expect(validator.errors).to include(a_string_matching(/service_id.*missing/))
      expect(validator.errors).to include(a_string_matching(/trip_id.*missing/))
    end

    it 'detects null values in required fields' do
      df_with_nulls = Polars::DataFrame.new({
        'route_id' => ['R1', nil],
        'service_id' => ['S1', 'S2'],
        'trip_id' => ['T1', 'T2']
      })
      validator = described_class.new(df_with_nulls, GtfsDf::Schema::Trips)
      expect(validator.valid?).to be false
      expect(validator.errors).to include(a_string_matching(/route_id.*null/))
    end

    it 'allows extra unexpected columns' do
      df_with_extra = Polars::DataFrame.new({
        'route_id' => ['R1'],
        'service_id' => ['S1'],
        'trip_id' => ['T1'],
        'extra_column' => ['Extra']
      })
      validator = described_class.new(df_with_extra, GtfsDf::Schema::Trips)
      expect(validator.valid?).to be true
    end

    it 'detects multiple enum validation errors' do
      df_with_multiple_invalid = Polars::DataFrame.new({
        'trip_id' => %w[T1 T2 T3],
        'stop_sequence' => [1, 2, 3],
        'stop_id' => %w[S1 S2 S3],
        'pickup_type' => ['0', '5', '9'],
        'drop_off_type' => ['1', '1', '1']
      })
      validator = described_class.new(df_with_multiple_invalid, GtfsDf::Schema::StopTimes)
      expect(validator.valid?).to be false
      expect(validator.errors.any? { |e| e.include?('pickup_type') && e.include?('5') }).to be true
    end
  end
end
