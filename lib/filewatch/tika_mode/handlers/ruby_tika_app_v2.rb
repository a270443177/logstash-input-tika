# frozen_string_literal: true

# Based on the rake remote task code

require 'rubygems'
require 'stringio'
require 'open3'

class RubyTikaApp
  TIKA_APP_VERSION = '2.9.1'

  class Error < RuntimeError; end

  class CommandFailedError < Error
    attr_reader :status
    def initialize(status)
      @status = status
    end
  end

  def initialize(document)
    @document = if (document =~ %r{https?:\/\/[\S]+}) == 0
                  document
                else
                  "#{document}"
                end


    
    gem_root_dir = Pathname.new(__FILE__).dirname.join("../../../../").realpath

    tika_path = gem_root_dir.join("lib/jars/tika-app-#{TIKA_APP_VERSION}.jar").expand_path.to_path
    tika_config_path = gem_root_dir.join("lib/jars/tika-config.xml").expand_path.to_path

    java_cmd = 'java'
    java_args = '-server -Djava.awt.headless=true -Dfile.encoding=UTF-8'
    # tika_path = "/usr/local/logstash-8.11.1/logstash-input-tika/vender/tika-app-#{TIKA_APP_VERSION}.jar"
    @tika_cmd = "#{java_cmd} #{java_args} -jar #{tika_path} "
  end

  def to_xml
    run_tika('--xml')
  end

  def to_html
    run_tika('--html')
  end

  def to_json(*_args)
    run_tika('--json')
  end

  def to_text
    run_tika('--text')
  end

  def to_text_main
    run_tika('--text-main')
  end

  def to_metadata
    run_tika('--metadata')
  end

  private

  def run_tika(option)
    final_cmd = "#{@tika_cmd} #{option} '#{@document}'"


    @p_in, @p_out, @p_err, waiter = Open3.popen3(final_cmd)


    stdout_result = @p_out.read.strip
    stderr_result = @p_err.read.strip

    if stdout_result.empty? && !stderr_result.empty?
      raise(CommandFailedError.new(stderr_result),
            "execution failed with status #{stderr_result}: #{final_cmd}")
    end

    stdout_result
  ensure
    @p_in.close
    @p_out.close
    @p_err.close
  end
end