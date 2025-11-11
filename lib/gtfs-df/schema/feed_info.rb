# frozen_string_literal: true

module GtfsDf
  module Schema
    class FeedInfo < BaseGtfsTable
      SCHEMA = {
        'feed_publisher_name' => Polars::String,
        'feed_publisher_url' => Polars::String,
        'feed_lang' => Polars::String,
        'default_lang' => Polars::String,
        'feed_start_date' => Polars::String,
        'feed_end_date' => Polars::String,
        'feed_version' => Polars::String,
        'feed_contact_email' => Polars::String,
        'feed_contact_url' => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[feed_publisher_name feed_publisher_url feed_lang].freeze
    end
  end
end
