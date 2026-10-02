# frozen_string_literal: true

module GtfsDf
  class Reader
    # Loads a GTFS zip file and returns a Feed
    #
    # @param zip_path [String] Path to the GTFS zip file
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @param relevant_files [Array<String>] A list of file names, useful to avoid loading tables you don't care about.
    # @return [Feed] The loaded GTFS feed
    def self.load_from_zip(zip_path, parse_times: false, relevant_files: nil, extra: nil)
      data = nil

      relevant_files ||= GtfsDf::Feed::GTFS_FILES.map { |name| "#{name}.txt" }
      relevant_files = relevant_files.to_set
      extra ||= {}
      extra_files = extra[:files] || []
      extra_classes = extra[:classes] || {}

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
            # - files neither relevant nor supported by extra
            # - empty feed files
            file_name_truncated = file_name.split(".").first # without the .txt extension
            relevant_or_extra = relevant_files.include?(file_name) || (extra_files.include?(file_name_truncated) && extra_classes.key?(file_name_truncated.to_sym))
            next unless relevant_or_extra && has_header?(entry)

            seen[file_name] = entry.name

            entry.extract(file_name, destination_directory: tmpdir)
          end
        end

        data = load_from_dir(tmpdir, parse_times:, relevant_files:, extra:)
      end

      data
    end

    # Loads a GTFS dir and returns a Feed
    #
    # @param dir_path [String] Path to the GTFS directory
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    # @param relevant_files [Array<String>] A list of file names, useful to avoid loading tables you don't care about.
    # @return [Feed] The loaded GTFS feed
    def self.load_from_dir(dir_path, parse_times: false, relevant_files: nil, extra: nil)
      relevant_files ||= GtfsDf::Feed::GTFS_FILES.map { |name| "#{name}.txt" }
      relevant_files = relevant_files.to_set
      extra ||= {}
      extra_files = extra[:files] || []
      extra_classes = extra[:classes] || {}

      data = {}
      (GtfsDf::Feed::GTFS_FILES + extra_files).each do |gtfs_file|
        basename = "#{gtfs_file}.txt"
        path = File.join(dir_path, basename)
        relevant_or_extra = relevant_files.include?(basename) || (extra_files.include?(gtfs_file) && extra_classes.key?(gtfs_file.to_sym))
        next unless relevant_or_extra && File.exist?(path)

        data[gtfs_file] = data_frame(gtfs_file, path, extra_classes)
      end

      # TODO: Pass extra along to feed; currently feed just drops the extra data
      GtfsDf::Feed.new(data, parse_times: parse_times)
    end

    private_class_method def self.data_frame(gtfs_file, path, extra_classes)
      custom_class = extra_classes[gtfs_file.to_sym]
      return custom_class.new(path).df if custom_class

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
