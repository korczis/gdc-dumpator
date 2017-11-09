require 'fileutils'
require 'json'
require 'gooddata'

USERNAME = ENV['GDC_USERNAME'] || 'bear@gooddata.com'
PASSWORD = ENV['GDC_PASSWORD'] || ''
SERVER = ENV['GDC_SERVER'] || 'https://instance-develop-45.dev.intgdc.com'
PID = ENV['GDC_PID'] || 'l739tknzsa2b6mp9m49e1s6m1s8jdmhi'

def fetch(client, link)
  puts "Fetching #{link}"

  res = client.get(link)

  items = res['objects']['items']

  next_link = res['objects']['paging']['next']
  items += fetch(client, next_link + '&include=predecessors') if next_link

  items
end

def main
  data_dir = File.join(File.dirname(__FILE__), 'data', PID)
  FileUtils.mkdir_p(data_dir)

  client = GoodData.connect(USERNAME, PASSWORD, server: SERVER, verify_ssl: false)

  response = client.get("/gdc/md/#{PID}/objects/query")

  links = response['about']['links']

  data = links.map do |link|
    begin
      category = link['category']
      items = fetch(client, link['link'].gsub('limit=5', 'limit=50') + '&include=predecessors')

      category_dir = File.join(data_dir, category)
      FileUtils.mkdir_p(category_dir)

      items.each do |item|
        identifier = item[category]['meta']['identifier']
        item_path = File.join(category_dir, "#{identifier}.json")

        puts "Writing #{item_path}"
        File.open(item_path, 'w') do |f|
          f.write(JSON.pretty_generate(item))
        end
      end

      [category, items]
    rescue => e
      puts e.inspect
      nil
    end
  end

  # Remove nil values
  data.compact!

  objects = data.map(&:last)
  objects.flatten!
  # pp objects

  lookup = {}
  objects.each do |obj|
    root = obj.keys.first
    obj_uri = obj[root]['meta']['uri']
    lookup[obj_uri] = obj
  end

  sorted = []
  processed = Set.new
  uris = lookup.keys
  while uri = uris.shift do
    toposort(processed, sorted, uri, lookup)
  end

  sorted.uniq!

  sorted.each do |uri|
    obj = lookup[uri]
    key = obj.keys.first
    raw = obj[key]

    puts "#{key}: #{raw['meta']['identifier']} - #{raw['meta']['uri']}"
  end
end

def toposort(processed, sorted, uri, lookup)
  obj = lookup[uri]

  if processed.include?(uri)
    return
  end

  if obj.nil?
    puts "Unable to find object #{uri}"
    return
  end

  root = obj.keys.first
  raw = obj[root]

  predecessors = raw['links']['predecessors'] || []
  predecessors.each do |predecessor|
    toposort(processed, sorted, predecessor, lookup)
  end

  processed.add(uri)

  sorted.push(uri)
end

main if __FILE__ == $PROGRAM_NAME
