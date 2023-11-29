# encoding: utf-8
require_relative "ruby_tika_app_v2"
require 'json'

module FileWatch module TikaMode module Handlers
  class ReadFile < Base
    # seek file to which ever is furthest: either current bytes read or sincedb position
    private
    def seek_to_furthest_position(watched_file)
      previous_pos = sincedb_collection.find(watched_file).position
      watched_file.file_seek([watched_file.bytes_read, previous_pos].max)
    end

    public
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)

        if watched_file.size_changed?

          ret = RubyTikaApp.new(watched_file.path)

          tika_output_hash = { "tika_out" => "#{ret.to_text}", "file_content_hash"=> "#{Base64.encode64(File.open(watched_file.path, 'rb').read).force_encoding('UTF-8')}" }



          #watched_file.listener.accept()

          watched_file.listener.accept(tika_output_hash.to_json)



          position = watched_file.last_stat_size
          sincedb_collection.store_last_read(watched_file.sincedb_key, position)
          # key = watched_file.sincedb_key
          # if sincedb_collection.get(key)
          #   sincedb_collection.reading_completed(key)
          #   sincedb_collection.clear_watched_file(key)
          # end
        end
        # if watched_file.all_read?
        #   watched_file.listener.eof
        #   watched_file.file_close
        #   key = watched_file.sincedb_key
        #   if sincedb_collection.get(key)
        #     sincedb_collection.reading_completed(key)
        #     sincedb_collection.clear_watched_file(key)
        #   end
        #   watched_file.listener.deleted
        #   # NOTE: on top of un-watching we should also remove from the watched files collection
        #   # if the file is getting deleted (on completion), that part currently resides in
        #   # DeleteCompletedFileHandler - triggered above using `watched_file.listener.deleted`
        #   watched_file.unwatch
        # end
      end
    end

    
    def log_error(msg, watched_file, error)
      details = { :path => watched_file.path,
                  :exception => error.class,
                  :message => error.message,
                  :backtrace => error.backtrace }
      if logger.debug?
        details[:file] = watched_file
      else
        details[:backtrace] = details[:backtrace].take(8) if details[:backtrace]
      end
      logger.error(msg, details)
    end
  end
end end end
