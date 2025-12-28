#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "fileutils"
require "gtfs_df"
require "whirly"

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: split_by_agency.rb -i <input-gtfs.zip> --ids NUMBERS"

  opts.on("-i", "--input PATH", "Path to the input GTFS file") do |v|
    options[:input] = v
  end
  opts.on("--ids IDS", "Comma-separated list of agency_ids") do |v|
    options[:ids] = v
  end
end.parse!

unless options[:input] && options[:ids]
  warn "Both --input and --ids are required."
  exit 1
end

input_path = File.expand_path(options[:input])
agency_ids = options[:ids].split(",")
output_dir = File.expand_path("./output", __dir__)
FileUtils.mkdir_p(output_dir)

feed = nil

Whirly.configure spinner: "dots", stop: "✓"

Whirly.start do
  Whirly.status = "Loading"

  start_time = Time.now
  feed = GtfsDf::Reader.load_from_zip(input_path)
  elapsed = Time.now - start_time

  Whirly.status = "Loaded (#{elapsed.round(2)}s)"
end

agency_ids.each do |agency_id|
  Whirly.start do
    output_path = File.join(output_dir, "#{agency_id}.zip")

    start_time = Time.now

    Whirly.status = "-> #{agency_id} filtering..."
    filtered_feed = feed.filter({"agency" => {"agency_id" => agency_id}})

    Whirly.status = "-> #{agency_id} writing..."
    GtfsDf::Writer.write_to_zip(filtered_feed, output_path)

    elapsed = Time.now - start_time

    Whirly.status = "-> #{agency_id}.zip (#{elapsed.round(2)}s)"
  end
end

puts "✓  Done, all files are stored in the output/ directory"
