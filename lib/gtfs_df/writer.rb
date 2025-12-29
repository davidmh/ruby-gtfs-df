# frozen_string_literal: true

module GtfsDf
  class Writer
    # Exports a Feed to a GTFS zip file
    #
    # @param feed [Feed] The GTFS feed to export
    # @param zip_path [String] The path where the zip file will be created
    def self.write_to_zip(feed, zip_path)
      require "stringio"
      require "zlib"
      FileUtils.mkdir_p(File.dirname(zip_path))
      Zip::File.open(zip_path, create: true) do |zipfile|
        GtfsDf::Feed::GTFS_FILES.each do |file|
          df = feed.send(file)
          next unless df.is_a?(Polars::DataFrame)

          # Convert time fields back to strings if parse_times was enabled
          if feed.parse_times
            df = format_time_fields(file, df)
          end

          # Write CSV to StringIO
          csv_io = StringIO.new
          df.write_csv(csv_io)
          csv_io.rewind

          # Write StringIO directly to zip
          zipfile.get_output_stream("#{file}.txt") { |f| f.write(csv_io.read) }
        end
      end
    end

    # Exports a Feed to a directory as individual text files
    #
    # @param feed [Feed] The GTFS feed to export
    # @param dir_path [String] The path where the directory will be created
    def self.write_to_dir(feed, dir_path)
      FileUtils.mkdir_p(dir_path)
      GtfsDf::Feed::GTFS_FILES.each do |file|
        df = feed.send(file)
        next unless df.is_a?(Polars::DataFrame)

        # Convert time fields back to strings if parse_times was enabled
        df = format_time_fields(file, df) if feed.parse_times

        # Write CSV directly to file
        df.write_csv(File.join(dir_path, "#{file}.txt"))
      end
    end

    # Formats time fields back to HH:MM:SS strings for a given GTFS file
    #
    # @param file [String] The GTFS file name (e.g., "stop_times")
    # @param df [Polars::DataFrame] The DataFrame to format
    # @return [Polars::DataFrame] DataFrame with time fields formatted as strings
    def self.format_time_fields(file, df)
      schema_class_name = file.split("_").map(&:capitalize).join
      schema_class = begin
        GtfsDf::Schema.const_get(schema_class_name)
      rescue
        nil
      end

      return df unless schema_class&.respond_to?(:time_fields)

      time_fields = schema_class.time_fields
      time_fields.each do |field|
        next unless df.columns.include?(field)
        df = df.with_columns(GtfsDf::Utils.as_time_string(field))
      end

      df
    end
  end
end
