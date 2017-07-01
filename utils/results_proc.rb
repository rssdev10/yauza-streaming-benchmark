#!/usr/bin/env ruby
#
# JSON results collector
#

require 'json'

# Collector of avr, min, max values
class Statistics < Array
  def read(obj)
    self << [obj['avr'], obj['min'], obj['max']].map(&:to_i)
  end

  def to_s
    join "\t"
  end
end

def parse_file(file_name)
  name = ''
  obj = JSON.parse(File.read(file_name).gsub('\'', '"'))

  obj['result'].map do |item|
    throughput = Statistics.new
    latency = Statistics.new

    name = item['name'].sub 'out-', ''
    total_time = item['totalTime']
    total_processed = item['totalProcessed']

    throughput.read(item['throughput'])
    latency.read(item['latency'])
    [name, total_time, total_processed, throughput, latency]
  end
end

results = Dir.glob('results-*.json')
             .select { |name| name.end_with? 'run02.json' }
             .map do |file_name|
               group = file_name.scan(/(?<=\.)\w+(?=\.)/).first
               [group, parse_file(file_name)]
             end

res = results.group_by { |item| item[0] }.map do |item|
  item[1].flat_map { |record| record[1].map { |in_rec| [record[0]] + in_rec } }
         .group_by { |record| record[0] + record[1] }
         .map { |group| group[1] }
end

puts %w[Sys Op Time Processed Thr_Avr Thr_Min Thr_Max Lat_Avr Lat_Min Lat_Max].join "\t"
puts res.flat_map { |i| i }
        .flat_map { |i| i }
        .group_by { |i| i[1] }
        .flat_map { |i| i[1] }
        .map { |item| item.join "\t" }.join "\n"
