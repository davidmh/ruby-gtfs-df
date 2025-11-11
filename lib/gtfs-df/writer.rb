# frozen_string_literal: true

module GtfsDf
  class Writer
    # Exports a Feed to a GTFS zip file
    def self.write_to_zip(feed, zip_path)
      require 'stringio'
      require 'zlib'
      FileUtils.mkdir_p(File.dirname(zip_path))
      Zip::File.open(zip_path, create: true) do |zipfile|
        GtfsDf::Feed::GTFS_FILES.each do |file|
          df = feed.send(file)
          next unless df.is_a?(Polars::DataFrame)

          # Write CSV to StringIO
          csv_io = StringIO.new
          df.write_csv(csv_io)
          csv_io.rewind

          # Write StringIO directly to zip
          zipfile.get_output_stream("#{file}.txt") { |f| f.write(csv_io.read) }
        end
      end
    end
  end
end
