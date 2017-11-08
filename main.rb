require 'fileutils'
require 'json'
require 'gooddata'

PID = 'la84vcyhrq8jwbu4wpipw66q2sqeb923'

def fetch(client, link)
  puts "Fetching #{link}"

  res = client.get(link)

  items = res['objects']['items']

  next_link = res['objects']['paging']['next']
  if next_link
   items += fetch(client, next_link)
  end

  items
end

def main
  data_dir = File.join(File.dirname(__FILE__), 'data', PID)
  FileUtils.mkdir_p(data_dir)

  client = GoodData.connect

  response = client.get("/gdc/md/#{PID}/objects/query")

  links = response['about']['links']

  data = links.map do |link|
    begin
      category = link['category']
      items = fetch(client,link['link'].gsub('limit=5', 'limit=50'))

      category_dir = File.join(data_dir, category)
      FileUtils.mkdir_p(category_dir)

      items.each do |item|
        identifier = item[category]['meta']['identifier']
        item_path = File.join(category_dir, "#{identifier}.json")

        puts "Writing #{item_path}"
        File.open(item_path,'w') do |f|
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

  # TODO: Fix order here!
end

if __FILE__ == $PROGRAM_NAME
  main
end
