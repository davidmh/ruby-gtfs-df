# frozen_string_literal: true

module GtfsDf
  class Reader
    # Loads a GTFS zip file and returns a Feed
    #
    # @param zip_path [String] Path to the GTFS zip file
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @param relevant_files [Array<String>] A list of file names, useful to avoid loading tables you don't care about.
    # @return [Feed] The loaded GTFS feed
    def self.load_from_zip(zip_path, parse_times: false, relevant_files: nil)
      data = nil

      relevant_files ||= GtfsDf::Feed::GTFS_FILES.map { |name| "#{name}.txt" }
      relevant_files = relevant_files.to_set

      seen = {}

      Dir.mktmpdir do |tmpdir|
        Zip::File.open(zip_path) do |zip_file|
          zip_file.each do |entry|
            # Extract files in nested directories into the root of the tmpdir
            file_name = File.basename(entry.name)

            if seen[file_name]
              raise GtfsDf::Error, "Found multiple instances of the same file: #{seen[file_name]} and #{entry.name}"
            end

            # We're skipping:
            # - unrelated files
            # - empty feed files
            next unless relevant_files.include?(file_name) && has_header?(entry)

            seen[file_name] = entry.name

            entry.extract(file_name, destination_directory: tmpdir)
          end
        end

        data = load_from_dir(tmpdir, parse_times:, relevant_files:)
      end

      data
    end

    # Loads a GTFS dir and returns a Feed
    #
    # @param dir_path [String] Path to the GTFS directory
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @param relevant_files [Array<String>] A list of file names, useful to avoid loading tables you don't care about.
    # @return [Feed] The loaded GTFS feed
    def self.load_from_dir(dir_path, parse_times: false, relevant_files: nil)
      relevant_files ||= GtfsDf::Feed::GTFS_FILES.map { |name| "#{name}.txt" }
      relevant_files = relevant_files.to_set

      data = {}
      GtfsDf::Feed::GTFS_FILES.each do |gtfs_file|
        basename = "#{gtfs_file}.txt"
        path = File.join(dir_path, basename)
        next unless relevant_files.include?(basename) && File.exist?(path)

        data[gtfs_file] = data_frame(gtfs_file, path)
      end

      GtfsDf::Feed.new(data, parse_times: parse_times)
    end

    private_class_method def self.data_frame(gtfs_file, path)
      schema_class_name = gtfs_file.split("_").map(&:capitalize).join
      GtfsDf::Schema.const_get(schema_class_name).new(path).df
    end

    private_class_method def self.has_header?(zip_entry)
      zip_entry
        .get_input_stream
        .readline
        .delete_prefix("\xEF\xBB\xBF".b) # BOM
        .strip != ""
    rescue
      false
    end
  end
end
