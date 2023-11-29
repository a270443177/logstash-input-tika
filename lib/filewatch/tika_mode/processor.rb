# encoding: utf-8
require 'filewatch/processor'
require_relative "handlers/base"
require_relative "handlers/read_file"
require_relative "handlers/unignore"
module FileWatch module TikaMode
  # Must handle
  #   :read_file
  class Processor < FileWatch::Processor

    def initialize_handlers(sincedb_collection, observer)
      # we deviate from the tail mode handler initialization here
      # by adding a reference to self so we can read the quit flag during a (depth first) read loop
      @read_file = Handlers::ReadFile.new(self, sincedb_collection, observer, @settings)
      @unignore = Handlers::Unignore.new(self, sincedb_collection, observer, @settings)

    end

    def unignore(watched_file)
      @unignore.handle(watched_file)
    end


    def read_file(watched_file)
      @read_file.handle(watched_file)
    end


    def process_all_states(watched_files)
      process_watched(watched_files)
      return if watch.quit?
      process_ignored(watched_files)
      return if watch.quit?
      process_active(watched_files)
    end

    private

    def process_watched(watched_files)
      logger.trace(__method__.to_s)
      # Handles watched_files in the watched state.
      # for a slice of them:
      #   move to the active state
      #   should never have been active before
      # how much of the max active window is available
      to_take = @settings.max_active - watched_files.count { |wf| wf.active? }
      if to_take > 0
        watched_files.select(&:watched?).take(to_take).each do |watched_file|
          begin
            logger.info("process_watched：#{watched_file}")
            restat(watched_file)
            watched_file.activate
          rescue Errno::ENOENT
            common_deleted_reaction(watched_file, __method__)
            next
          rescue => e
            common_error_reaction(watched_file, e, __method__)
            next
          end
          break if watch.quit?
        end
      else
        now = Time.now.to_i
        if (now - watch.lastwarn_max_files) > MAX_FILES_WARN_INTERVAL
          waiting = watched_files.size - @settings.max_active
          logger.warn("#{@settings.max_warn_msg}, files yet to open: #{waiting}")
          watch.lastwarn_max_files = now
        end
      end
    end


    def process_ignored(watched_files)
      logger.trace(__method__.to_s)
      # Handles watched_files in the ignored state.
      # if its size changed:
      #   put it in the watched state
      #   invoke unignore
      watched_files.each do |watched_file|
        next unless watched_file.ignored?
        common_restat_with_delay(watched_file, __method__) do
          # it won't do this if rotation is detected
          if watched_file.size_changed?
            watched_file.watch
            unignore(watched_file)
          end
        end
        break if watch.quit?
      end
    end
    ## TODO add process_rotation_in_progress

    def process_active(watched_files)
      logger.trace(__method__.to_s)
      # Handles watched_files in the active state.
      watched_files.each do |watched_file|
        next unless watched_file.active?

        begin
          restat(watched_file)
        rescue Errno::ENOENT
          common_deleted_reaction(watched_file, __method__)
          next
        rescue => e
          common_error_reaction(watched_file, e, __method__)
          next
        end
        break if watch.quit?

        read_file(watched_file)

        if @settings.exit_after_read
          common_detach_when_allread(watched_file)
        end
        # handlers take care of closing and unwatching
      end
    end



    def common_detach_when_allread(watched_file)
      watched_file.unwatch
      watched_file.listener.reading_completed
      add_deletable_path watched_file.path
      logger.trace? && logger.trace("whole file read, removing from collection", :path => watched_file.path)
    end

    def common_deleted_reaction(watched_file, action)
      # file has gone away or we can't read it anymore.
      watched_file.unwatch
      add_deletable_path watched_file.path
      logger.trace? && logger.trace("#{action} - stat failed, removing from collection", :path => watched_file.path)
    end

    def common_error_reaction(watched_file, error, action)
      logger.error("#{action} - other error", error_details(error, watched_file))
    end

    def common_restat_with_delay(watched_file, action, &block)
      common_restat(watched_file, action, true, &block)
    end

    def common_restat_without_delay(watched_file, action, &block)
      common_restat(watched_file, action, false, &block)
    end

    def common_restat(watched_file, action, delay, &block)
      all_ok = true
      begin
        restat(watched_file)
        if watched_file.rotation_in_progress?
          logger.trace("-------------------- >>>>> restat - rotation_detected", :watched_file => watched_file.details, :new_sincedb_key => watched_file.stat_sincedb_key)
          # don't yield to closed and ignore processing
        else
          yield if block_given?
        end
      rescue Errno::ENOENT
        if delay
          logger.trace("#{action} - delaying the stat fail on", :filename => watched_file.filename)
          watched_file.delay_delete
        else
          # file has gone away or we can't read it anymore.
          logger.trace("#{action} - after a delay, really can't find this file", :path => watched_file.path)
          watched_file.unwatch
          logger.trace("#{action} - removing from collection", :filename => watched_file.filename)
          delete(watched_file)
          add_deletable_path watched_file.path
          all_ok = false
        end
      rescue => e
        logger.error("#{action} - other error", error_details(e, watched_file))
        all_ok = false
      end
      all_ok
    end

  end
end end
