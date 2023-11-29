while true
do
  LOG_AT=ERROR /usr/local/logstash-8.11.1/bin/ruby -S  bundle exec rspec -fd --fail-fast --tag ~lsof ./spec || break
done
