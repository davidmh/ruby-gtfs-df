# frozen_string_literal: true

module GtfsDf
  class Reader
    # Loads a GTFS zip file and returns a Feed
    #
    # @param zip_path [String] Path to the GTFS zip file
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @return [Feed] The loaded GTFS feed
    def self.load_from_zip(zip_path, parse_times: false)
      data = nil

      Dir.mktmpdir do |tmpdir|
        Zip::File.open(zip_path) do |zip_file|
          zip_file.each do |entry|
            next unless entry.file?
            out_path = File.join(tmpdir, entry.name)
            entry.extract(out_path)
          end
        end

        data = load_from_dir(tmpdir, parse_times: parse_times)
      end

      data
    end

    # Loads a GTFS dir and returns a Feed
    #
    # @param dir_path [String] Path to the GTFS directory
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @return [Feed] The loaded GTFS feed
    def self.load_from_dir(dir_path, parse_times: false)
      data = {}
      GtfsDf::Feed::GTFS_FILES.each do |gtfs_file|
        path = File.join(dir_path, "#{gtfs_file}.txt")
        next unless File.exist?(path)

        data[gtfs_file] = data_frame(gtfs_file, path)
      end

      GtfsDf::Feed.new(data, parse_times: parse_times)
    end

    private_class_method def self.data_frame(gtfs_file, path)
      schema_class_name = gtfs_file.split("_").map(&:capitalize).join
      GtfsDf::Schema.const_get(schema_class_name).new(path).df
    end
  end
end
