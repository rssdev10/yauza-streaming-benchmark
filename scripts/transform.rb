#!/usr/bin/env ruby

INPUT_DIR = 'output/'

CUR_DIR = File.dirname(__FILE__)

LOGS = Dir.glob(CUR_DIR + '/../' + INPUT_DIR + '*.log').sort

puts LOGS

params = {
  cpu: [1, 2],
  cpu_usr: [1],
  memory: [7],
  network: [11, 12],
  disk: [13, 14]
}

out_files = params.each_key.reduce({}) do |acc, key|
  acc.merge(key => File.open("report_#{key}.csv", 'w'))
end

def parse_line(str)
  str.split(',').map(&:to_f)
end

def read_and_parse(file)
  begin
    parse_line(file.readline)
  rescue StandardError
    []
  end
end

record = []

files = LOGS.map do |fn|
  puts fn
  file = File.open(fn)
  #  headers = []
  data = []

  begin
    # headers = data
    str = file.readline
    data = str.split ','
  end while data[0].to_i.zero? && !file.eof?

  # puts headers
  record << data.map(&:to_f)
  file
end

until files.any?(&:eof?)
  time_0 = record[0][0]

  1.upto(files.size - 1) do |i|
    loop do
      time = record[i][0]
      if (files[i].eof? || record[i].empty? || time_0 - 1 <= time)
        break
      end
      record[i] = read_and_parse(files[i])
    end
  end

  if record.all?(&:any?)
    params.each do |key, nums|
      res = [time_0.round] << record.map do |item|
        if (item[0] - time_0).abs < 1
          nums.map { |num| item[num] }.reduce(:+).round
        else
          nil
        end
      end
      str = res.join ','
      out_files[key].puts str
    end

    record[0] = read_and_parse(files[0])
  end
end

files.each(&:close)
out_files.each_value(&:close)
