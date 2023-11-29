# encoding: utf-8
require "logstash/util/loggable"
require_relative "tika_mode/processor"

module FileWatch
  class ObservingTika
    include LogStash::Util::Loggable
    include ObservingBase

    def subscribe(observer)
      # observer here is the file input
      watch.subscribe(observer, sincedb_collection)
      sincedb_collection.write("read mode subscribe complete - shutting down")
    end

    private

    def build_specific_processor(settings)
      TikaMode::Processor.new(settings)
    end
  end
end
